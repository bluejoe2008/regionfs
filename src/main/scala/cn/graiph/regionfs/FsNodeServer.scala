package cn.graiph.regionfs

import java.io._
import java.util.{Properties, Random}

import cn.graiph.regionfs.util.{Configuration, ConfigurationEx, Logging}
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcCallContext, RpcEndpoint, RpcEnv, RpcEnvServerConfig}
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
    val props = new Properties()
    val fis = new FileInputStream(configFile)
    props.load(fis)
    fis.close()

    val conf = new ConfigurationEx(new Configuration {
      override def getRaw(name: String): Option[String] =
        if (props.containsKey(name))
          Some(props.getProperty(name))
        else
          None
    })

    val storeDir = conf.getRequiredValueAsFile("data.storeDir", configFile.getParentFile).
      getCanonicalFile.getAbsoluteFile

    if (!storeDir.exists())
      throw new RegionFsServersException(s"store dir does not exist: ${storeDir.getPath}")

    //TODO: use a leader node: manages all regions
    new FsNodeServer(
      conf.getRequiredValueAsString("zookeeper.address"),
      conf.getRequiredValueAsInt("node.id"),
      storeDir,
      conf.getValueAsString("server.host", "localhost"),
      conf.getValueAsInt("server.port", 1224)
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
    val localRegionManager = new RegionManager(nodeId, storeDir)
    var rpcEnv: RpcEnv = null

    val zk = new ZooKeeper(zks, 2000, new Watcher {
      override def process(event: WatchedEvent): Unit = {
      }
    })

    //get neighbour nodes
    val neighbourNodes = new WatchingNodes(zk, !address.equals(_))
    //get regions in neighbour nodes
    val neighbourRegions = new WatchingRegions(zk, !address.equals(_))

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
      val queue = new FileTaskQueue()
      val rand = new Random();

      //NOTE: register only on started up
      override def onStart(): Unit = {
        //register this node and regions
        zk.create(s"/regionfs/nodes/$addrString", "".getBytes,
          Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
        registerExsitingRegions()
      }

      private def createNewRegion(): Region = {
        val region = localRegionManager.createNew()
        val regionId = region.regionId

        //notify neighbours
        //TODO: replica number?
        //find thinnest neighbour which has least regions
        if (!neighbourRegions.map.isEmpty) {
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
          Await.result(thinNeighbourClient.endPointRef.ask[CreateRegionResponse](CreateRegionRequest(regionId)),
            Duration.apply("30s"))
        }

        //ok, now I register this region
        registerNewRegion(regionId)
        region
      }

      private def chooseRegion(): Region = {
        localRegionManager.synchronized {
          //counterOffset=size of region
          //TODO: sort on idle
          localRegionManager.regions.values.toArray.sortBy(_.length).headOption.
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
        case CreateRegionRequest(regionId: Long) => {
          localRegionManager.createNewReplica(regionId)
          context.reply(CreateRegionResponse(regionId))
        }

        case SendCompleteFileRequest(optRegionId: Option[Long], block: Array[Byte], totalLength: Long) => {
          val (regionId, localId) = {
            //primary node
            if (!optRegionId.isDefined) {
              val region = chooseRegion()
              val maybeLocalId = new FileTask(region, totalLength).
                writeChunk(0, block, 0, totalLength.toInt, 0)
              val neighbours = getNeighboursWhoHasRegion(region.regionId)
              //ask neigbours
              val futures = neighbours.map(addr =>
                neighbourNodes.clientOf(addr).endPointRef.ask[SendCompleteFileResponse](
                  SendCompleteFileRequest(Some(region.regionId), block, totalLength)))

              //wait all neigbours' replies
              futures.map(future =>
                Await.result(future, Duration.Inf))

              region.regionId -> maybeLocalId.get
            }
            else {
              val region = localRegionManager.get(optRegionId.get)
              val maybeLocalId = new FileTask(region, totalLength).
                writeChunk(0, block, 0, totalLength.toInt, 0)

              region.regionId -> maybeLocalId.get
            }
          }

          context.reply(SendCompleteFileResponse(FileId.make(regionId, localId)))
        }

        case StartSendChunksRequest(optRegionId: Option[Long], totalLength: Long) => {
          val region = optRegionId.map(localRegionManager.get(_)).getOrElse(chooseRegion())
          val (transId, task) = queue.create(localRegionManager.get(region.regionId), totalLength)

          if (!optRegionId.isDefined) {
            //notify neighbours
            val neighbours = getNeighboursWhoHasRegion(region.regionId)
            val futures = neighbours.map(addr =>
              addr -> neighbourNodes.clientOf(addr).endPointRef.ask[StartSendChunksResponse](
                StartSendChunksRequest(Some(region.regionId), totalLength)))

            //wait all neigbours' reply
            val transIds = futures.map(x =>
              x._1 -> Await.result(x._2, Duration.Inf))

            //save these transIds from neighbour
            transIds.foreach(x => task.addNeighbourTransactionId(x._1, x._2.transId))
          }

          context.reply(StartSendChunksResponse(transId))
        }

        case SendChunkRequest(transId: Long, chunkBytes: Array[Byte], offset: Long, chunkLength: Int, chunkIndex: Int) => {
          val task = queue.get(transId)
          val opt = task.writeChunk(transId, chunkBytes, offset, chunkLength, chunkIndex)
          opt.foreach(_ => queue.remove(transId))

          //notify neighbours
          val ids = task.getNeighbourTransactionIds()

          val futures = ids.map(x => neighbourNodes.clientOf(x._1).endPointRef.ask[SendChunkResponse](
            SendChunkRequest(x._2, chunkBytes, offset, chunkLength, chunkIndex)))

          //TODO: sync()?
          futures.foreach(Await.result(_, Duration.Inf))
          context.reply(SendChunkResponse(opt.map(FileId.make(task.region.regionId, _)), chunkLength))
        }

        case ReadCompleteFileRequest(regionId: Long, localId: Long) => {
          // get region
          val region = localRegionManager.get(regionId)
          val content: Array[Byte] = region.read(localId)

          context.reply(ReadCompleteFileResponse(content))
        }
      }

      override def onStop(): Unit = {
        println("stop endpoint")
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

    private def registerExsitingRegions(): Unit = {
      localRegionManager.regions.keys.foreach(regionId => {
        registerNewRegion(regionId)
      })
    }

    private def registerNewRegion(regionId: Long) = {
      zk.create(s"/regionfs/regions/${addrString}_$regionId", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    }

  }

}




