package cn.graiph.regionfs

import java.io._
import java.util.Random

import cn.graiph.regionfs.util.{ConfigurationEx, Logging}
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcCallContext, RpcEndpoint, RpcEnv, RpcEnvServerConfig}
import org.apache.commons.io.IOUtils
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Created by bluejoe on 2019/8/22.
  */
class RegionFsServersException(msg: String, cause: Throwable = null) extends RuntimeException(msg, cause) {
}

/**
  * FsNodeServer factory
  */
object FsNodeServer {
  /**
    * create a FsNodeServer with a configuration file, e.g. node1.conf
    */
  def build(configFile: File): FsNodeServer = {
    val conf = new ConfigurationEx(configFile)
    val storeDir = conf.get("data.storeDir").asFile(configFile.getParentFile).
      getCanonicalFile.getAbsoluteFile

    if (!storeDir.exists())
      throw new RegionFsServersException(s"store dir does not exist: ${storeDir.getPath}")

    //TODO: use a leader node: manages all regions
    new FsNodeServer(
      conf.get("zookeeper.address").asString,
      conf.get("node.id").asInt,
      storeDir,
      conf.get("server.host").withDefault("localhost").asString,
      conf.get("server.port").withDefault(1224).asInt
    )
  }
}

/**
  * a FsNodeServer responds blob save/read requests
  */
class FsNodeServer(zks: String, nodeId: Int, storeDir: File, host: String, port: Int) extends Logging {
  var rpcServer: FsRpcServer = null
  val addrString = s"${host}_$port"

  logger.debug(s"nodeId: ${nodeId}")
  logger.debug(s"storeDir: ${storeDir.getCanonicalFile.getAbsolutePath}")

  def start() {
    rpcServer = new FsRpcServer()
    logger.info(s"starting fs-node on $host:$port")
    rpcServer.start()
  }

  def shutdown(): Unit = {
    if (rpcServer != null)
      rpcServer.shutdown()
  }

  class FsRpcServer() {
    val address = NodeAddress(host, port)
    val zk = new ZooKeeper(zks, 2000, new Watcher {
      override def process(event: WatchedEvent): Unit = {
      }
    })
    val globalConfig = GlobalConfig.load(zk)
    val localRegionManager = new RegionManager(nodeId, storeDir, globalConfig)
    var rpcEnv: RpcEnv = null

    //get neighbour nodes
    val neighbourNodes = new NodeWatcher(zk, !address.equals(_))
    //get regions in neighbour nodes
    val neighbourRegions = new RegionWatcher(zk, !address.equals(_))

    prepareZkEntries

    def start() {
      val config = RpcEnvServerConfig(new RpcConf(), "regionfs-server", host, port)
      rpcEnv = NettyRpcEnvFactory.create(config)
      val endpoint: RpcEndpoint = new FileRpcEndpoint(rpcEnv)
      rpcEnv.setupEndpoint("regionfs-service", endpoint)
      rpcEnv.awaitTermination()
    }

    def shutdown(): Unit = {
      if (rpcEnv != null)
        rpcEnv.shutdown()
    }

    class FileRpcEndpoint(override val rpcEnv: RpcEnv)
      extends RpcEndpoint with Logging {
      val queue = new TxQueue()
      val transactions = new Transactions();
      val rand = new Random();

      //NOTE: register only on started up
      override def onStart(): Unit = {
        //register this node and regions
        zk.create(s"/regionfs/nodes/$addrString", "".getBytes,
          Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
        syncExsitingRegions()
      }

      private def createNewRegion(): Region = {
        val region = localRegionManager.createNew()
        val regionId = region.regionId

        //notify neighbours
        //find thinnest neighbour which has least regions
        if (globalConfig.replicaNum > 1) {
          if (neighbourRegions.map.size < globalConfig.replicaNum - 1)
            throw new InsufficientNodeServerException(globalConfig.replicaNum);

          val thinNeighbourClient = neighbourRegions.map.
            groupBy(_._1).
            map(x => x._1 -> x._2.size).
            toArray.
            sortBy(_._2).
            map(_._1).
            headOption.
            map(neighbourNodes.clientOf(_)).
            getOrElse(
              //no node found, so throw a dice
              neighbourNodes.clients.toArray.apply(rand.nextInt(neighbourNodes.size))
            )

          //hello, pls create a new region with id=regionId
          thinNeighbourClient.ask[CreateRegionResponse](CreateRegionRequest(regionId),
            Duration.apply("30s"))
        }

        //ok, now I register this region
        syncZkRegion(regionId)
        region
      }

      private def chooseRegion(): Region = {
        localRegionManager.synchronized {
          //counterOffset=size of region
          //TODO: sort on idle
          localRegionManager.regions.values.toArray.sortBy(_.statTotalSize).headOption.
            getOrElse({
              //no enough regions
              createNewRegion()
            })
        }
      }

      private def getNeighboursWhoHasRegion(regionId: Long): Array[NodeAddress] = {
        //a region may be stored in multiple nodes
        neighbourRegions.map.filter(_._2 == regionId).filter(_._1 != address).map(_._1).toArray
      }

      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case ListFileRequest() => {
          context.reply(ListFileResponse(localRegionManager.regions.values.flatMap(_.listFiles).toArray))
        }

        case GetNodeStatRequest() => {
          val nodeStat = NodeStat(nodeId, address,
            localRegionManager.regions.map { kv =>
              RegionStat(kv._1, kv._2.statFileCount(), kv._2.statTotalSize())
            }.toList)

          context.reply(GetNodeStatResponse(nodeStat))
        }

        case CreateRegionRequest(regionId: Long) => {
          localRegionManager.createNewReplica(regionId)
          syncZkRegion(regionId)
          context.reply(CreateRegionResponse(regionId))
        }

        case SendCompleteFileRequest(optRegionId: Option[Long], block: Array[Byte], totalLength: Long) => {
          val (regionId, localId) = {
            //primary node
            if (!optRegionId.isDefined) {
              val region = chooseRegion()
              val maybeLocalId = new TransTx(-1, region, totalLength).
                writeChunk(0, block, 0, totalLength.toInt, 0)
              val neighbours = getNeighboursWhoHasRegion(region.regionId)
              //ask neigbours
              val futures = neighbours.map(addr =>
                neighbourNodes.clientOf(addr).askAsync[SendCompleteFileResponse](
                  SendCompleteFileRequest(Some(region.regionId), block, totalLength)))

              //wait all neigbours' replies
              futures.map(future =>
                Await.result(future, Duration.Inf))

              region.regionId -> maybeLocalId.get
            }
            else {
              val region = localRegionManager.get(optRegionId.get)
              val maybeLocalId = new TransTx(-1, region, totalLength).
                writeChunk(0, block, 0, totalLength.toInt, 0)

              region.regionId -> maybeLocalId.get
            }
          }

          context.reply(SendCompleteFileResponse(FileId.make(regionId, localId)))
        }

        case StartSendChunksRequest(optRegionId: Option[Long], totalLength: Long) => {
          val region = optRegionId.map(localRegionManager.get(_)).getOrElse(chooseRegion())
          val tx = queue.create(localRegionManager.get(region.regionId), totalLength)

          if (!optRegionId.isDefined) {
            //notify neighbours
            val neighbours = getNeighboursWhoHasRegion(region.regionId)
            val futures = neighbours.map(addr =>
              addr -> neighbourNodes.clientOf(addr).askAsync[StartSendChunksResponse](
                StartSendChunksRequest(Some(region.regionId), totalLength)))

            //wait all neigbours' reply
            val transIds = futures.map(x =>
              x._1 -> Await.result(x._2, Duration.Inf))

            //save these transIds from neighbour
            transIds.foreach(x => tx.addNeighbourTransactionId(x._1, x._2.transId))
          }

          context.reply(StartSendChunksResponse(tx.txId))
        }

        case SendChunkRequest(transId: Long, chunkBytes: Array[Byte], offset: Long, chunkLength: Int, chunkIndex: Int) => {
          val task = queue.get(transId)
          val opt = task.writeChunk(transId, chunkBytes, offset, chunkLength, chunkIndex)
          opt.foreach(_ => queue.remove(transId))

          //notify neighbours
          val ids = task.getNeighbourTransactionIds()

          val futures = ids.map(x => neighbourNodes.clientOf(x._1).askAsync[SendChunkResponse](
            SendChunkRequest(x._2, chunkBytes, offset, chunkLength, chunkIndex)))

          //TODO: sync()?
          futures.foreach(Await.result(_, Duration.Inf))
          context.reply(SendChunkResponse(opt.map(FileId.make(task.region.regionId, _)), chunkLength))
        }

        case ReadChunkRequest(regionId: Long, localId: Long, offset: Long, chunkLength: Long) => {
          // get region
          val region = localRegionManager.get(regionId)
          val content: Array[Byte] = region.read(localId, offset, chunkLength, (is: InputStream) => {
            IOUtils.toByteArray(is)
          })

          context.reply(ReadChunkResponse(content,
            if (content.length < chunkLength) {
              -1
            }
            else {
              offset + content.length
            }))
        }

        case StartStreamRequest(request: AnyRef, pageSize: Int) => {
          val tx = transactions.create(createProducer(request), pageSize)
          val (results, hasMorePage) = tx.nextPage
          context.reply(StreamResponse(tx.txId, results.toArray, hasMorePage))
        }

        case GetNextPageRequest(txId: Long) => {
          val tx = transactions.get(txId);
          val (results, hasMorePage) = tx.nextPage
          context.reply(StreamResponse(txId, results.toArray, hasMorePage))
        }
      }

      override def onStop(): Unit = {
        logger.info("stop endpoint")
      }

      private def createProducer(request: AnyRef): (Output) => Unit = {
        request match {
          case ListFileRequest() => {
            (out: Output) => {
              localRegionManager.regions.values.foreach { x =>
                val it = x.listFiles()
                it.foreach(out.push(_))
              }

              out.markEOF();
            }
          }
        }
      }
    }

    private def prepareZkEntries(): Unit = {
      if (zk.exists("/regionfs", false) == null)
        zk.create("/regionfs", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)

      if (zk.exists("/regionfs/nodes", false) == null)
        zk.create("/regionfs/nodes", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)

      if (zk.exists("/regionfs/regions", false) == null)
        zk.create("/regionfs/regions", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    }

    private def syncExsitingRegions(): Unit = {
      localRegionManager.regions.keys.foreach(regionId => {
        syncZkRegion(regionId)
      })
    }

    private def syncZkRegion(regionId: Long) = {
      zk.create(s"/regionfs/regions/${addrString}_$regionId", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    }
  }

}

class InsufficientNodeServerException(num: Int) extends
  RegionFsServersException(s"insufficient node server for replica: num>=$num") {

}




