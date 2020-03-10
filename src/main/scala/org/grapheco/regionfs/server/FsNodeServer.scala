package org.grapheco.regionfs.server

import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.{HippoRpcEnv, HippoRpcEnvFactory}
import net.neoremind.kraps.rpc.{RpcAddress, RpcCallContext, RpcEndpoint, RpcEnvServerConfig}
import org.apache.commons.io.IOUtils
import org.grapheco.commons.util.{ConfigurationEx, Logging, ProcessUtils}
import org.grapheco.hippo.{ChunkedStream, CompleteStream, HippoRpcHandler, ReceiveContext}
import org.grapheco.regionfs._
import org.grapheco.regionfs.client._
import org.grapheco.regionfs.util.{CrcUtils, ParsedChildNodeEventHandler, RegionFsException, ZooKeeperClient}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Created by bluejoe on 2019/8/22.
  */
/**
  * FsNodeServer factory
  */
object FsNodeServer {
  /**
    * create a FsNodeServer with a configuration file, e.g. node1.conf
    */
  private def create(conf: ConfigurationEx, baseDir: File): FsNodeServer = {
    val storeDir = conf.get("data.storeDir").asFile(baseDir).
      getCanonicalFile.getAbsoluteFile

    if (!storeDir.exists())
      throw new StoreDirNotExistsException(storeDir)

    val lockFile = new File(storeDir, ".lock")
    if (lockFile.exists()) {
      val fis = new FileInputStream(lockFile)
      val pid = IOUtils.toString(fis).toInt
      fis.close()

      throw new StoreLockedException(storeDir, pid)
    }

    //TODO: use a leader node: manages all regions
    new FsNodeServer(
      conf.get("zookeeper.address").asString,
      conf.get("node.id").asInt,
      storeDir,
      conf.get("server.host").withDefault("localhost").asString,
      conf.get("server.port").withDefault(1224).asInt
    )
  }

  def create(props: Map[String, String], baseDir: File = null): FsNodeServer = {
    create(new ConfigurationEx(props), baseDir)
  }

  def create(configFile: File): FsNodeServer = {
    create(new ConfigurationEx(configFile), configFile.getParentFile)
  }
}

/**
  * a FsNodeServer responds blob save/read requests
  */
class FsNodeServer(zks: String, nodeId: Int, storeDir: File, host: String, port: Int) extends Logging {
  logger.debug(s"nodeId: ${nodeId}")
  logger.debug(s"storeDir: ${storeDir.getCanonicalFile.getAbsolutePath}")

  val zookeeper = ZooKeeperClient.create(zks)
  val (env, address) = createRpcEnv(zookeeper)
  val globalConfig = zookeeper.loadGlobalConfig()

  val localRegionManager = new RegionManager(nodeId, storeDir, globalConfig, new RegionEventListener {
    override def handleRegionEvent(event: RegionEvent): Unit = {
      event match {
        case CreateRegionEvent(region) => {
          //registered
        }

        case WriteRegionEvent(region) => {
          zookeeper.writeRegionData(nodeId, region)
        }
      }
    }
  })

  val clientFactory = new FsNodeClientFactory(globalConfig);

  //get neighbour nodes
  val cachedClients = mutable.Map[Int, FsNodeClient]()
  val neighbourNodes = mutable.Map[Int, RpcAddress]()
  val neighbourNodesWatcher = zookeeper.watchNodeList(
    new ParsedChildNodeEventHandler[(Int, RpcAddress)] {
      override def onCreated(t: (Int, RpcAddress)): Unit = {
        neighbourNodes += t
      }

      override def onDeleted(t: (Int, RpcAddress)): Unit = {
        neighbourNodes -= t._1
      }

      override def accepts(t: (Int, RpcAddress)): Boolean = {
        nodeId != t._1
      }
    })

  //get regions in neighbour nodes
  //32768->(1,2), 32769->(1), ...
  var neighbourRegionWithNodes = mutable.Map[Long, ArrayBuffer[Int]]()
  var neighbourNodeWithRegionCount = mutable.ListMap[Int, AtomicInteger]()

  val neighbourRegionsWatcher = zookeeper.watchRegionList(
    new ParsedChildNodeEventHandler[(Long, Int)] {
      override def onCreated(t: (Long, Int)): Unit = {
        neighbourNodeWithRegionCount.getOrElseUpdate(t._2, new AtomicInteger(0)).incrementAndGet()
        neighbourRegionWithNodes.getOrElseUpdate(t._1, ArrayBuffer()) += t._2
      }

      override def onDeleted(t: (Long, Int)): Unit = {
        neighbourNodeWithRegionCount(t._2).decrementAndGet()
        neighbourRegionWithNodes(t._1) -= t._2
      }

      override def accepts(t: (Long, Int)): Boolean = {
        nodeId != t._2
      }
    })

  var alive: Boolean = true
  val endpoint = new FileRpcEndpoint(env)
  env.setupEndpoint("regionfs-service", endpoint)
  env.setRpcHandler(endpoint)
  writeLockFile(new File(storeDir, ".lock"))

  def awaitTermination(): Unit = {
    println(IOUtils.toString(this.getClass.getClassLoader.getResourceAsStream("logo.txt"), "utf-8"))
    println(s"starting node server on ${address}, nodeId=${nodeId}, storeDir=${storeDir.getAbsoluteFile.getCanonicalPath}")

    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run(): Unit = {
        shutdown();
      }
    })

    env.awaitTermination()
  }

  def shutdown(): Unit = {
    if (alive) {
      clientFactory.close()
      neighbourNodesWatcher.close()
      neighbourRegionsWatcher.close()

      new File(storeDir, ".lock").delete();
      env.shutdown()
      zookeeper.close()
      println(s"shutdown node server on ${address}, nodeId=${nodeId}")
      alive = false;
    }
  }

  private def clientOf(nodeId: Int): FsNodeClient = {
    cachedClients.getOrElseUpdate(nodeId,
      clientFactory.of(neighbourNodes(nodeId)))
  }

  def cleanData(): Unit = {
    throw new NotImplementedError();
  }

  private def writeLockFile(lockFile: File): Unit = {
    val pid = ProcessUtils.getCurrentPid();
    val fos = new FileOutputStream(lockFile);
    fos.write(pid.toString.getBytes())
    fos.close()
  }

  private def createRpcEnv(zookeeper: ZooKeeperClient): (HippoRpcEnv, RpcAddress) = {
    val env = HippoRpcEnvFactory.create(
      RpcEnvServerConfig(new RpcConf(), "regionfs-server", host, port))

    val address = env.address
    val path = s"/regionfs/nodes/${nodeId}_${address.host}_${address.port}"
    zookeeper.assertPathNotExists(path) {
      env.shutdown()
    }
    env -> address;
  }

  class FileRpcEndpoint(override val rpcEnv: HippoRpcEnv)
    extends RpcEndpoint
      with HippoRpcHandler
      with Logging {

    val traffic = new AtomicInteger(0);

    //NOTE: register only on started up
    override def onStart(): Unit = {
      //register this node and regions
      zookeeper.createNodeNode(nodeId, address);
      localRegionManager.regions.foreach(x => zookeeper.createRegionNode(nodeId, x._2))
    }

    private def createNewRegion(): (Region, Array[Int]) = {
      val region = localRegionManager.createNew()
      val regionId = region.regionId

      val nodeIds: Array[Int] = {
        if (globalConfig.replicaNum <= 1) {
          Array[Int]()
        }
        else {
          if (neighbourNodes.size < globalConfig.replicaNum - 1)
            throw new InsufficientNodeServerException(neighbourNodes.size, globalConfig.replicaNum);

          //notify neighbours
          //find thinnest neighbour which has least regions

          //TODO: very very time costing
          val thinNodeIds = neighbourNodes.map(
            x => x._1 -> neighbourNodeWithRegionCount.getOrElse(x._1, new AtomicInteger(0)).get).
            toList.sortBy(_._2).takeRight(globalConfig.replicaNum - 1).map(_._1)

          if (logger.isTraceEnabled()) {
            logger.trace(s"chosen thin nodes: ${thinNodeIds.mkString(",")}");
          }

          val futures = thinNodeIds.map(clientOf(_).endPointRef.ask[CreateRegionResponse](
            CreateRegionRequest(regionId)))

          //hello, pls create a new region with id=regionId
          futures.foreach(Await.result(_, Duration.Inf))

          thinNodeIds.toArray
        }
      }

      //ok, now I register this region
      zookeeper.createRegionNode(nodeId, region)
      region -> nodeIds
    }

    private def chooseRegionForWrite(): (Region, Array[Int]) = {
      localRegionManager.synchronized {
        localRegionManager.regions.values.toArray.
          filter(x => x.isPrimary && x.isWritable).
          sortBy(_.length).headOption.
          map(region => {
            region -> neighbourRegionWithNodes.get(region.regionId).map(_.toArray).getOrElse(Array[Int]())
          }).
          getOrElse({
            //no enough regions
            createNewRegion()
          })
      }
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case x: Any => {
        traffic.incrementAndGet()
        val t = receiveAndReplyInternal(context).apply(x)
        traffic.decrementAndGet()
        t
      }
    }

    override def onStop(): Unit = {
      logger.info("stop endpoint")
    }

    override def openCompleteStream(): PartialFunction[Any, CompleteStream] = {
      case x: Any => {
        traffic.incrementAndGet()
        val t = openCompleteStreamInternal.apply(x)
        traffic.decrementAndGet()
        t
      }
    }

    override def openChunkedStream(): PartialFunction[Any, ChunkedStream] = {
      case x: Any => {
        traffic.incrementAndGet()
        val t = openChunkedStreamInternal.apply(x)
        traffic.decrementAndGet()
        t
      }
    }

    override def receiveWithStream(extraInput: ByteBuffer, context: ReceiveContext): PartialFunction[Any, Unit] = {
      case x: Any => {
        traffic.incrementAndGet()
        val t = receiveWithStreamInternal(extraInput, context).apply(x)
        traffic.decrementAndGet()
        t
      }
    }

    private def receiveAndReplyInternal(ctx: RpcCallContext): PartialFunction[Any, Unit] = {
      case GetNodeStatRequest() => {
        val nodeStat = NodeStat(nodeId, address,
          localRegionManager.regions.map { kv =>
            RegionStat(kv._1, kv._2.fileCount, kv._2.length)
          }.toList)

        ctx.reply(GetNodeStatResponse(nodeStat))
      }

      //create region as replica
      case CreateRegionRequest(regionId: Long) => {
        val region = localRegionManager.createNewReplica(regionId)
        zookeeper.createRegionNode(nodeId, region)
        ctx.reply(CreateRegionResponse(regionId))
      }

      case PrepareToWriteFileRequest(fileSize: Long) => {
        val (region: Region, nodeIds: Array[Int]) = chooseRegionForWrite()
        ctx.reply(PrepareToWriteFileResponse(
          region.regionId,
          region.peekNextFileId(),
          (Set(region.nodeId) ++ nodeIds).toArray)
        )
      }

      case ShutdownRequest() => {
        ctx.reply(ShutdownResponse(address))
        shutdown()
      }

      case CleanDataRequest() => {
        cleanData()
        ctx.reply(CleanDataResponse(address))
      }

      case DeleteFileRequest(regionId: Long, localId: Long) => {
        val maybeRegion = localRegionManager.get(regionId)
        if (maybeRegion.isEmpty) {
          throw new WrongRegionIdException(regionId);
        }

        try {
          maybeRegion.get.delete(localId)
          //notify neigbours
          //TODO: filter(ownsNewVersion)
          val futures = neighbourRegionWithNodes(regionId).filter(_ => true).map(clientOf(_).endPointRef.ask[CreateRegionResponse](
            DeleteFileRequest(regionId, localId)))

          //TODO: if fail?
          futures.foreach(Await.result(_, Duration("4s")))
          ctx.reply(DeleteFileResponse(true, null))
        }
        catch {
          case e: Throwable =>
            ctx.reply(DeleteFileResponse(false, e.getMessage))
        }
      }

      case GreetingRequest(msg: String) => {
        println(s"node-${nodeId}($address): \u001b[31;47;4m${msg}\u0007\u001b[0m")
        ctx.reply(GreetingResponse(address))
      }
    }

    private def openCompleteStreamInternal(): PartialFunction[Any, CompleteStream] = {
      case ReadFileRequest(regionId: Long, localId: Long) => {
        val maybeRegion = localRegionManager.get(regionId)

        if (maybeRegion.isEmpty) {
          throw new WrongRegionIdException(regionId);
        }

        val maybeBuffer = maybeRegion.get.read(localId)
        if (maybeBuffer.isEmpty) {
          throw new WrongLocalIdException(regionId, localId);
        }

        CompleteStream.fromByteBuffer(maybeBuffer.get)
      }
    }

    private def openChunkedStreamInternal(): PartialFunction[Any, ChunkedStream] = {
      case ListFileRequest() =>
        ChunkedStream.pooled[ListFileResponseDetail](1024, (pool) => {
          localRegionManager.regions.values.foreach { x =>
            val it = x.listFiles()
            it.foreach(x => pool.push(ListFileResponseDetail(x)))
          }
        })
    }

    private def receiveWithStreamInternal(extraInput: ByteBuffer, context: ReceiveContext): PartialFunction[Any, Unit] = {
      case SendFileRequest(regionId: Long, fileId: FileId, totalLength: Long, crc32: Long) =>
        //primary region
        val region = localRegionManager.get(regionId).get
        val clone = extraInput.duplicate()

        if (globalConfig.enableCrc && CrcUtils.computeCrc32(clone) != crc32) {
          throw new ReceiveTimeMismatchedCheckSumException();
        }

        val localId = region.write(extraInput, crc32)
        context.reply(SendFileResponse(FileId.make(regionId, localId)))
    }
  }

}

class RegionFsServerException(msg: String, cause: Throwable = null) extends
  RegionFsException(msg, cause) {
}

class InsufficientNodeServerException(actual: Int, required: Int) extends
  RegionFsServerException(s"insufficient node server for replica: actual: ${actual}, required: ${required}") {

}

class StoreLockedException(storeDir: File, pid: Int) extends
  RegionFsServerException(s"store is locked by another node server: node server pid=${pid}, storeDir=${storeDir.getPath}") {

}

class StoreDirNotExistsException(storeDir: File) extends
  RegionFsServerException(s"store dir does not exist: ${storeDir.getPath}") {

}

class WrongLocalIdException(regionId: Long, localId: Long) extends
  RegionFsServerException(s"file #${localId} not exist in region #${regionId}") {

}

class WrongRegionIdException(regionId: Long) extends
  RegionFsServerException(s"region not exist: ${regionId}") {

}

class ReceiveTimeMismatchedCheckSumException extends
  RegionFsServerException(s"mismatched checksum exception on receive time") {

}