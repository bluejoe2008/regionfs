package cn.graiph.blobfs

import java.io._
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

import cn.graiph.blobfs.util.{Configuration, ConfigurationEx, Logging}
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcCallContext, RpcEndpoint, RpcEnv, RpcEnvServerConfig}
import org.apache.commons.io.IOUtils
import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Random

class BlobFsServersException(msg: String, cause: Throwable = null) extends RuntimeException(msg, cause) {
}

/**
  * Created by bluejoe on 2019/8/22.
  */
object FsNodeServer {
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
      throw new BlobFsServersException(s"store dir does not exist: ${storeDir.getPath}")

    new FsNodeServer(
      conf.getRequiredValueAsString("zookeeper.address"),
      conf.getRequiredValueAsInt("node.id"),
      storeDir,
      conf.getValueAsString("server.host", "localhost"),
      conf.getValueAsInt("server.port", 1224)
    )
  }
}

class FsNodeServer(zks: String, nodeId: Int, storeDir: File, host: String, port: Int) extends Logging {
  var rpcServer: FsRpcServer = null
  val addrString = s"${host}_$port"
  logger.debug(s"nodeId: ${nodeId}")
  logger.debug(s"storeDir: ${storeDir.getCanonicalFile.getAbsolutePath}")

  def start() {
    val zk = new ZooKeeper(zks, 2000, new Watcher {
      override def process(event: WatchedEvent): Unit = {
      }
    })

    if (zk.exists("/blobfs", false) == null)
      zk.create("/blobfs", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)

    if (zk.exists("/blobfs/nodes", false) == null)
      zk.create("/blobfs/nodes", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)

    zk.create(s"/blobfs/nodes/$addrString", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)

    if (zk.exists("/blobfs/regions", false) == null)
      zk.create("/blobfs/regions", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)

    rpcServer = new FsRpcServer(zk, new RegionManager(storeDir))
    logger.info(s"starting fs-node on $host:$port")
    rpcServer.start()
  }

  def shutdown(): Unit = {
    if (rpcServer != null)
      rpcServer.shutdown()
  }

  class FsRpcServer(zk: ZooKeeper, localRegionManager: RegionManager)
    extends Logging {
    val address = NodeAddress(host, port)
    val cachedNeighbourClients = mutable.Map[NodeAddress, FsNodeClient]()
    var rpcEnv: RpcEnv = null

    val bufferedNeighbours = ArrayBuffer[NodeAddress]()
    val bufferedNeighbourRegions = ArrayBuffer[(NodeAddress, Long)]()

    private def zkUpdateRegions(): Unit = {
      localRegionManager.regions.keys.foreach(regionId => {
        zkAddRegion(regionId)
      })
    }

    private def zkAddRegion(regionId: Long) = {
      zk.create(s"/blobfs/regions/${addrString}_$regionId", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    }

    private def zkLoadNeighbours(): Unit = {
      val children = zk.getChildren(s"/blobfs/nodes", new Watcher {
        override def process(event: WatchedEvent): Unit = {
          println(event);
          if (event.getType == EventType.NodeChildrenChanged) {
            zkLoadNeighbours();
          }
        }
      })

      val neighbours: Iterable[NodeAddress] = children.filterNot(addrString.equals(_)).map { name =>
        NodeAddress.fromString(name, ":")
      }

      bufferedNeighbours.clear()
      bufferedNeighbours ++= neighbours
    }

    private def zkLoadNeighbourRegions(): Unit = {
      val children = zk.getChildren(s"/blobfs/regions", new Watcher {
        override def process(event: WatchedEvent): Unit = {
          println(event);
          if (event.getType == EventType.NodeChildrenChanged) {
            zkLoadNeighbourRegions();
          }
        }
      })

      val nodeRegions: Iterable[(NodeAddress, Long)] =
        children.
          filterNot(_.startsWith(addrString)).
          map { name =>
            val splits = name.split("_")
            NodeAddress(splits(0), splits(1).toInt) -> splits(2).toLong
          }

      logger.debug(s"loaded neighbour regions: $nodeRegions")
      bufferedNeighbourRegions.clear()
      bufferedNeighbourRegions ++= nodeRegions
    }

    def start() {
      zkLoadNeighbourRegions()
      zkUpdateRegions()

      val config = RpcEnvServerConfig(new RpcConf(), "blobfs-server", host, port)
      rpcEnv = NettyRpcEnvFactory.create(config)
      val endpoint: RpcEndpoint = new FileRpcEndpoint(rpcEnv)
      rpcEnv.setupEndpoint("blobfs-service", endpoint)
      rpcEnv.awaitTermination()
    }

    def shutdown(): Unit = {
      if (rpcEnv != null)
        rpcEnv.shutdown()
    }

    def getNeighbourClient(nodeServerAddress: NodeAddress): FsNodeClient = {
      cachedNeighbourClients.getOrElseUpdate(nodeServerAddress, {
        FsNodeClient.connect(nodeServerAddress)
      })
    }

    class FileRpcEndpoint(override val rpcEnv: RpcEnv)
      extends RpcEndpoint with Logging {
      val queue = new FileTaskQueue()
      val rand = new Random();

      override def onStart(): Unit = {
        logger.info(s"endpoint started")
      }

      private def createNewRegion(): Region = {
        //get max region ids
        val regionId: Long = (localRegionManager.regions.map(_._1).toArray.sortWith(_ > _).
          headOption.getOrElse(nodeId.toLong << 16)) + 1
        val region = localRegionManager.createNew(regionId)

        //notify neighbour
        //TODO: replica number?
        //find thinnest neighbour which has least regions
        val thinNeighbour = bufferedNeighbourRegions.
          groupBy(_._1).
          map(x => x._1 -> x._2.size).
          toArray.
          sortBy(_._2).
          map(_._1).
          headOption.
          getOrElse(
            //no node found, so throw a dice
            bufferedNeighbours.get(rand.nextInt(bufferedNeighbours.size))
          )

        Await.result(getNeighbourClient(thinNeighbour).endPointRef.ask[CreateRegionResponse](CreateRegionRequest(regionId)),
          Duration.apply("30s"))

        zkAddRegion(regionId)
        region
      }

      private def chooseRegion(): Region = {
        localRegionManager.regions.values.toArray.sortBy(_.counterOffset.get).headOption.
          getOrElse({
            //no enough region
            createNewRegion()
          })
      }

      private def getNeighboursHasRegion(regionId: Long): Array[NodeAddress] = {
        bufferedNeighbourRegions.filter(_._2 == regionId).filter(_._1 != address).map(_._1).toArray
      }

      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case CreateRegionRequest(regionId: Long) => {
          localRegionManager.createNew(regionId)
          context.reply(CreateRegionResponse(regionId))
        }

        case SendCompleteFileRequest(optRegionId: Option[Long], block: Array[Byte], totalLength: Long) => {
          //choose a region
          val region = optRegionId.map(localRegionManager.get(_)).getOrElse(chooseRegion())
          val opt = new queue.FileTask(region, totalLength).
            write(0, block, 0, totalLength.toInt, 0)

          val localId = opt.get

          if (!optRegionId.isDefined) {
            val neighbours = getNeighboursHasRegion(region.regionId)
            //ask neigbours
            val futures = neighbours.map(addr =>
              getNeighbourClient(addr).endPointRef.ask[SendCompleteFileResponse](
                SendCompleteFileRequest(Some(region.regionId), block, totalLength)))

            //wait all neigbours' reply
            futures.map(future =>
              Await.result(future, Duration.apply("30s")))
          }

          context.reply(SendCompleteFileResponse(FileId.make(region.regionId, localId)))
        }

        case StartSendChunksRequest(optRegionId: Option[Long], totalLength: Long) => {
          val region = optRegionId.map(localRegionManager.get(_)).getOrElse(chooseRegion())
          val (transId, task) = queue.create(localRegionManager.get(region.regionId), totalLength)

          if (!optRegionId.isDefined) {
            //notify neighbours
            val neighbours = getNeighboursHasRegion(region.regionId)
            val futures = neighbours.map(addr =>
              addr -> getNeighbourClient(addr).endPointRef.ask[StartSendChunksResponse](
                StartSendChunksRequest(Some(region.regionId), totalLength)))

            //wait all neigbours' reply
            val transIds = futures.map(x =>
              x._1 -> Await.result(x._2, Duration.apply("30s")))

            //save these transIds from neighbour
            transIds.foreach(x => task.addNeighbourTransactionId(x._1, x._2.transId))
          }

          context.reply(StartSendChunksResponse(transId))
        }

        case SendChunkRequest(transId: Long, chunkBytes: Array[Byte], offset: Long, chunkLength: Int, chunkIndex: Int) => {
          val task = queue.get(transId)
          val opt = task.write(transId, chunkBytes, offset, chunkLength, chunkIndex)
          opt.foreach(_ => queue.remove(transId))

          //notify neighbours
          val ids = task.getNeighbourTransactionIds()

          val futures = ids.map(x => getNeighbourClient(x._1).endPointRef.ask[SendChunkResponse](
            SendChunkRequest(x._2, chunkBytes, offset, chunkLength, chunkIndex)))

          //TODO: sync()?
          futures.foreach(Await.result(_, Duration.apply("30s")))
          context.reply(SendChunkResponse(opt.map(FileId.make(task.region.regionId, _)), chunkLength))
        }
      }

      override def onStop(): Unit = {
        println("stop endpoint")
      }
    }

    class FileTaskQueue() extends Logging {
      val transactionalTasks = mutable.Map[Long, FileTask]()
      val transactionIdGen = new AtomicLong(System.currentTimeMillis())

      def create(region: Region, totalLength: Long): (Long, FileTask) = {
        val task = new FileTask(region, totalLength)
        val transId = transactionIdGen.incrementAndGet()
        transactionalTasks += transId -> task
        (transId, task)
      }

      def remove(transId: Long) = transactionalTasks.remove(transId)

      def get(transId: Long): FileTask = {
        transactionalTasks(transId)
      }

      class FileTask(val region: Region, val totalLength: Long) {
        val neighbourTransactionIds = mutable.Map[NodeAddress, Long]()

        def addNeighbourTransactionId(address: NodeAddress, transId: Long): Unit = {
          neighbourTransactionIds += address -> transId
        }

        def getNeighbourTransactionIds() = neighbourTransactionIds.toMap

        case class Chunk(file: File, length: Int, index: Int) {
        }

        //create a new file
        val chunks = ArrayBuffer[Chunk]()
        val actualBytesWritten = new AtomicLong(0)

        private def combine(transId: Long): File = {
          if (chunks.length == 1) {
            chunks(0).file
          }
          else {
            //create combined file
            val tmpFile = File.createTempFile(s"blobfs-$transId-", "")
            val fos: FileOutputStream = new FileOutputStream(tmpFile, true)
            chunks.sortBy(_.index).foreach { chunk =>
              val cis = new FileInputStream(chunk.file)
              IOUtils.copy(cis, fos)
              cis.close()
              chunk.file.delete()
            }

            fos.close()
            tmpFile
          }
        }

        def write(transId: Long, chunkBytes: Array[Byte], offset: Long, chunkLength: Int, chunkIndex: Int): Option[Long] = {
          logger.debug(s"writing chunk: $transId-$chunkIndex, length=$chunkLength")

          val tmpFile = this.synchronized {
            File.createTempFile(s"blobfs-$transId-", ".chunk")
          }

          println(tmpFile.getName)
          val fos: FileOutputStream = new FileOutputStream(tmpFile)
          IOUtils.copy(new ByteArrayInputStream(chunkBytes.slice(0, chunkLength)), fos)
          fos.close()

          chunks.synchronized {
            chunks += Chunk(tmpFile, chunkLength, chunkIndex)
          }

          val actualBytes = actualBytesWritten.addAndGet(chunkLength)

          //end of file?
          if (actualBytes >= totalLength) {
            val combinedFile = combine(transId);
            val localId = region.save(
              () => new FileInputStream(combinedFile),
              actualBytes,
              None)

            combinedFile.delete()
            Some(localId)
          }
          else {
            None
          }
        }
      }

    }

  }

}




