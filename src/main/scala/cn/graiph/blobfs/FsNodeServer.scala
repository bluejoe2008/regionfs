package cn.graiph.blobfs

import java.io._
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

import cn.graiph.blobfs.util.{Configuration, ConfigurationEx, Logging}
import com.google.gson.Gson
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcCallContext, RpcEndpoint, RpcEnv, RpcEnvServerConfig}
import org.apache.commons.io.IOUtils
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}

import scala.collection.mutable.ArrayBuffer
import scala.collection.{JavaConversions, mutable}
import scala.concurrent.Await
import scala.concurrent.duration.Duration

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
      storeDir,
      conf.getValueAsString("server.host", "localhost"),
      conf.getValueAsInt("server.port", 1224)
    )
  }
}

class FsNodeServer(zks: String, storeDir: File, host: String, port: Int) extends Logging {
  var rpcServer: FsRpcServer = null
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

    if (zk.exists("/blobfs/regions", false) == null)
      zk.create("/blobfs/regions", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)

    val regionManager = new RegionManager(storeDir)
    val json = new Gson().toJson(JavaConversions.mapAsJavaMap(
      Map("regions" -> regionManager.regions.keys.toArray, "rpcAddress" -> s"$host:$port"))).getBytes

    val nodeId = s"${host}_${port}"
    zk.create(s"/blobfs/nodes/$nodeId", json, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)

    rpcServer = new FsRpcServer(host, port, regionManager)
    logger.info(s"starting fs-node on $host:$port")
    rpcServer.start()
  }

  def shutdown(): Unit = {
    if (rpcServer != null)
      rpcServer.shutdown()
  }
}

class FsRpcServer(host: String, port: Int, regionManager: RegionManager)
  extends Logging {

  val neighbourClients = mutable.Map[NodeAddress, FsNodeClient]()
  var rpcEnv: RpcEnv = null

  def start() {
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
    neighbourClients.getOrElseUpdate(nodeServerAddress, {
      FsNodeClient.connect(nodeServerAddress)
    })
  }

  class FileRpcEndpoint(override val rpcEnv: RpcEnv)
    extends RpcEndpoint with Logging {
    val queue = new FileTaskQueue()

    override def onStart(): Unit = {
      logger.info(s"endpoint started")
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case GetNodeStatRequest() => {
        context.reply(GetNodeStatResponse(
          NodeStat(
            (regionManager.regions.values.map(_.regionId) ++ Array(0)).max,
            regionManager.regions.size,
            regionManager.regions.values.map(x =>
              RegionStat(x.regionId, x.counterOffset.get)).toArray
          )
        )
        )
      }

      case CreateRegionRequest(regionId: Int) => {
        regionManager.createNew(regionId)
        context.reply(CreateRegionResponse(regionId))
      }

      case SendCompleteFileRequest(neighbours: Array[NodeAddress], regionId: Int, block: Array[Byte], totalLength: Long) => {
        val opt = new queue.FileTask(regionManager.get(regionId), totalLength).
          write(0, block, 0, totalLength.toInt, 0)

        val localId = opt.get
        //TODO: sync()
        //ask neigbours
        val futures = neighbours.map(addr =>
          getNeighbourClient(addr).endPointRef.ask[SendCompleteFileResponse](
            SendCompleteFileRequest(Array(), regionId, block, totalLength)))

        //wait all neigbours' reply
        futures.map(future =>
          Await.result(future, Duration.apply("30s")))

        context.reply(SendCompleteFileResponse(localId))
      }

      case StartSendChunksRequest(neighbours: Array[NodeAddress], regionId: Int, totalLength: Long) => {
        val (transId, task) = queue.create(regionManager.get(regionId), totalLength)

        //notify neighbours
        val futures = neighbours.map(addr =>
          addr -> getNeighbourClient(addr).endPointRef.ask[StartSendChunksResponse](
            StartSendChunksRequest(Array(), regionId, totalLength)))

        //wait all neigbours' reply
        val transIds = futures.map(x =>
          x._1 -> Await.result(x._2, Duration.apply("30s")))

        //save these transIds from neighbour
        transIds.foreach(x => task.addNeighbourTransactionId(x._1, x._2.transId))
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
        context.reply(SendChunkResponse(opt, chunkLength))
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

    class FileTask(region: Region, totalLength: Long) {
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

      def write(transId: Long, chunkBytes: Array[Byte], offset: Long, chunkLength: Int, chunkIndex: Int): Option[Int] = {
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

          Some(localId)
        }
        else {
          None
        }
      }
    }
  }
}


