package org.grapheco.regionfs.server

import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import io.netty.buffer.Unpooled
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.{HippoRpcEnv, HippoRpcEnvFactory}
import net.neoremind.kraps.rpc.{RpcAddress, RpcCallContext, RpcEndpoint, RpcEnvServerConfig}
import org.apache.commons.io.IOUtils
import org.grapheco.commons.util.{ConfigurationEx, Logging, ProcessUtils}
import org.grapheco.hippo.{ChunkedStream, CompleteStream, HippoRpcHandler, ReceiveContext}
import org.grapheco.regionfs._
import org.grapheco.regionfs.client._
import org.grapheco.regionfs.util._

import scala.collection.mutable
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
    val storeDir = conf.get(Constants.PARAMETER_KEY_DATA_STORE_DIR).asFile(baseDir).
      getCanonicalFile.getAbsoluteFile

    if (!storeDir.exists())
      throw new StoreDirectoryNotExistsException(storeDir)

    val lockFile = new File(storeDir, ".lock")
    if (lockFile.exists()) {
      val fis = new FileInputStream(lockFile)
      val pid = IOUtils.toString(fis).toInt
      fis.close()

      throw new RegionStoreLockedException(storeDir, pid)
    }

    new FsNodeServer(
      conf.get(Constants.PARAMETER_KEY_ZOOKEEPER_ADDRESS).asString,
      conf.get(Constants.PARAMETER_KEY_NODE_ID).asInt,
      storeDir,
      conf.get(Constants.PARAMETER_KEY_SERVER_HOST).withDefault(Constants.DEFAULT_SERVER_HOST).asString,
      conf.get(Constants.PARAMETER_KEY_SERVER_PORT).withDefault(Constants.DEFAULT_SERVER_PORT).asInt
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
  val globalSetting = zookeeper.loadGlobalSetting()

  val localRegionManager = new LocalRegionManager(nodeId, storeDir, globalSetting, new RegionEventListener {
    override def handleRegionEvent(event: RegionEvent): Unit = {
    }
  })

  val clientFactory = new FsNodeClientFactory(globalSetting);

  //get neighbour nodes
  val cachedClients = mutable.Map[Int, FsNodeClient]()
  val mapNeighbourNodeWithAddress = mutable.Map[Int, RpcAddress]()
  val mapNeighbourNodeWithRegionCount = mutable.SortedSet[(Int, Int)]()(Ordering.by(_._2))
  val zkNodeEventHandlers = new CompositeParsedChildNodeEventHandler[NodeServerInfo]();

  zkNodeEventHandlers.addHandler(new ParsedChildNodeEventHandler[NodeServerInfo] {
    override def onCreated(t: NodeServerInfo): Unit = {
      mapNeighbourNodeWithAddress += t.nodeId -> t.address
      mapNeighbourNodeWithRegionCount += t.nodeId -> t.regionCount
    }

    def onUpdated(t: NodeServerInfo): Unit = {
      mapNeighbourNodeWithRegionCount += t.nodeId -> t.regionCount
    }

    def onInitialized(batch: Iterable[NodeServerInfo]): Unit = {
      mapNeighbourNodeWithAddress.synchronized {
        mapNeighbourNodeWithAddress.clear()
        mapNeighbourNodeWithAddress ++= batch.map(t => t.nodeId -> t.address)
        mapNeighbourNodeWithRegionCount ++= batch.map(t => t.nodeId -> t.regionCount)

      }
    }

    override def onDeleted(t: NodeServerInfo): Unit = {
      mapNeighbourNodeWithAddress -= t.nodeId
      mapNeighbourNodeWithRegionCount -= t.nodeId -> t.regionCount
    }
  })

  //region1->[node1->revision1,node2->revision2]
  val mapRegionWithNodes = mutable.Map[Long, mutable.Map[Int, Long]]()

  var alive: Boolean = true
  val endpoint = new FileRpcEndpoint(env)
  env.setupEndpoint("regionfs-service", endpoint)
  env.setRpcHandler(endpoint)
  writeLockFile(new File(storeDir, ".lock"))

  val remoteRegionWatcher: RemoteRegionWatcher = new RemoteRegionWatcher(nodeId,
    globalSetting, zkNodeEventHandlers, localRegionManager, clientOf);

  val neighbourNodesWatcher = zookeeper.watchNodeList(nodeId != _.nodeId, zkNodeEventHandlers)

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
      clientFactory.close
      neighbourNodesWatcher.close
      remoteRegionWatcher.close

      new File(storeDir, ".lock").delete()
      env.shutdown()
      zookeeper.close()
      println(s"shutdown node server on ${address}, nodeId=${nodeId}")
      alive = false;
    }
  }

  private def clientOf(nodeId: Int): FsNodeClient = {
    cachedClients.synchronized {
      cachedClients.getOrElseUpdate(nodeId,
        clientFactory.of(mapNeighbourNodeWithAddress(nodeId)))
    }
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

    private val traffic = new AtomicInteger(0);

    //NOTE: register only on started up
    override def onStart(): Unit = {
      //register this node
      zookeeper.createNodeNode(nodeId, address, localRegionManager);
    }

    private def createNewRegion(): (Region, Array[RegionInfo]) = {
      val region = localRegionManager.createNew()
      val regionId = region.regionId

      val infos: Array[RegionInfo] = {
        if (globalSetting.replicaNum <= 1) {
          Array()
        }
        else {
          if (mapNeighbourNodeWithAddress.size < globalSetting.replicaNum - 1)
            throw new InsufficientNodeServerException(mapNeighbourNodeWithAddress.size, globalSetting.replicaNum);

          val thinNodeIds = mapNeighbourNodeWithRegionCount.take(globalSetting.replicaNum - 1).map(_._1)
          val futures = thinNodeIds.map(clientOf(_).createSecondaryRegion(regionId))

          //hello, pls create a new region with id=regionId
          val results = futures.map(Await.result(_, Duration.Inf).info).toArray
          zookeeper.updateNodeData(nodeId, address, localRegionManager)
          remoteRegionWatcher.cacheRemoteSeconaryRegions(results)
          results
        }
      }

      region -> infos
    }

    private def chooseRegionForWrite(): (Region, Array[RegionInfo]) = {
      val writableRegions = localRegionManager.regions.values.toArray.filter(x => x.isWritable && x.isPrimary);

      //too few writable regions
      if (writableRegions.size < globalSetting.minWritableRegions) {
        createNewRegion()
      }
      else {
        val region = localRegionManager.synchronized {
          localRegionManager.regions(localRegionManager.ring.take(x =>
            writableRegions.exists(_.regionId == x)).get)
        }

        region -> remoteRegionWatcher.getAvailableRegions(region.regionId)
      }
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case x: Any => {
        try {
          traffic.incrementAndGet()
          val t = receiveAndReplyInternal(context).apply(x)
          traffic.decrementAndGet()
          t
        }
        catch {
          case e: Throwable =>
            context.sendFailure(e)
        }
        finally {
          traffic.decrementAndGet()
        }
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
        try {
          traffic.incrementAndGet()
          val t = receiveWithStreamInternal(extraInput, context).apply(x)
          t
        }
        catch {
          case e: Throwable =>
            context.sendFailure(e)
        }
        finally {
          traffic.decrementAndGet()
        }
      }
    }

    private def receiveAndReplyInternal(ctx: RpcCallContext): PartialFunction[Any, Unit] = {

      case GetRegionOwnerNodesRequest(regionId: Long) => {
        ctx.reply(GetRegionOwnerNodesResponse(localRegionManager.regions.get(regionId).map(_.info).toArray
          ++ remoteRegionWatcher.cachedRemoteSecondaryRegions(regionId)))
      }

      case GetRegionsOnNodeRequest() => {
        ctx.reply(GetRegionsOnNodeResponse(localRegionManager.regions.values.map(_.info).toArray))
      }

      case GetNodeStatRequest() => {
        val nodeStat = NodeStat(nodeId, address,
          localRegionManager.regions.map { kv =>
            RegionStat(kv._1, kv._2.fileCount, kv._2.length)
          }.toList)

        ctx.reply(GetNodeStatResponse(nodeStat))
      }

      case GetRegionInfoRequest(regionIds: Array[Long]) => {
        val infos = regionIds.map(regionId => {
          val region = localRegionManager.regions(regionId)
          region.info
        })

        ctx.reply(GetRegionInfoResponse(infos))
      }

      //create region as replica
      case CreateSecondaryRegionRequest(regionId: Long) => {
        val region = localRegionManager.createSecondaryRegion(regionId)
        zookeeper.updateNodeData(nodeId, address, localRegionManager)
        ctx.reply(CreateSecondaryRegionResponse(region.info))
      }

      case ShutdownRequest() => {
        ctx.reply(ShutdownResponse(address))
        shutdown()
      }

      case CleanDataRequest() => {
        cleanData()
        ctx.reply(CleanDataResponse(address))
      }

      case RegisterSeconaryRegionsRequest(regions: Array[RegionInfo]) => {
        remoteRegionWatcher.cacheRemoteSeconaryRegions(regions)
      }

      case DeleteSeconaryFileRequest(fileId: FileId) => {
        val maybeRegion = localRegionManager.get(fileId.regionId)
        if (maybeRegion.isEmpty) {
          throw new WrongRegionIdException(nodeId, fileId.regionId);
        }

        try {
          if (maybeRegion.get.revision <= fileId.localId)
            throw new FileNotFoundException(nodeId, fileId);

          maybeRegion.get.delete(fileId.localId)
          ctx.reply(DeleteSeconaryFileResponse(true, null, maybeRegion.get.info))
        }
        catch {
          case e: Throwable =>
            ctx.reply(DeleteSeconaryFileResponse(false, e.getMessage, maybeRegion.get.info))
        }
      }

      case DeleteFileRequest(fileId: FileId) => {
        val maybeRegion = localRegionManager.get(fileId.regionId)
        if (maybeRegion.isEmpty) {
          throw new WrongRegionIdException(nodeId, fileId.regionId);
        }

        try {
          if (maybeRegion.get.revision <= fileId.localId)
            throw new FileNotFoundException(nodeId, fileId);

          maybeRegion.get.delete(fileId.localId)
          val regions =
          //is a primary region?
            if (globalSetting.replicaNum > 1 && (fileId.regionId >> 16) == nodeId) {
              //notify secondary regions
              val futures = remoteRegionWatcher.getAvailableRegions(fileId.regionId).map(x =>
                clientOf(x.nodeId).endPointRef.ask[DeleteSeconaryFileResponse](
                  DeleteSeconaryFileRequest(fileId)))

              //TODO: if fail?
              futures.map(Await.result(_, Duration.Inf).info)
            }
            else {
              Array(maybeRegion.get.info)
            }

          remoteRegionWatcher.cacheRemoteSeconaryRegions(regions)
          ctx.reply(DeleteFileResponse(true, null, regions))
        }
        catch {
          case e: Throwable =>
            ctx.reply(DeleteFileResponse(false, e.getMessage, Array()))
        }
      }

      case GreetingRequest(msg: String) => {
        println(s"node-${nodeId}($address): \u001b[31;47;4m${msg}\u0007\u001b[0m")
        ctx.reply(GreetingResponse(address))
      }
    }

    private def openCompleteStreamInternal(): PartialFunction[Any, CompleteStream] = {
      case ReadFileRequest(fileId: FileId) => {
        val maybeRegion = localRegionManager.get(fileId.regionId)

        if (maybeRegion.isEmpty) {
          throw new WrongRegionIdException(nodeId, fileId.regionId);
        }

        val maybeBuffer = maybeRegion.get.read(fileId.localId)
        if (maybeBuffer.isEmpty) {
          throw new FileNotFoundException(nodeId, fileId);
        }

        val body = Unpooled.wrappedBuffer(maybeBuffer.get.duplicate())
        val crc32 = CrcUtils.computeCrc32(maybeBuffer.get.duplicate())

        CompleteStream.fromByteBuffer(ReadFileResponseHead(body.readableBytes(), crc32,
          remoteRegionWatcher.getAvailableRegions(fileId.regionId)), body)
      }

      case GetRegionPatchRequest(regionId: Long, since: Long) => {
        val maybeRegion = localRegionManager.get(regionId)
        if (maybeRegion.isEmpty) {
          throw new WrongRegionIdException(nodeId, regionId);
        }

        if (traffic.get() > Constants.MAX_BUSY_TRAFFIC) {
          CompleteStream.fromByteBuffer(Unpooled.buffer(1024).writeByte(Constants.MARK_GET_REGION_PATCH_SERVER_IS_BUSY))
        }
        else {
          val buf = maybeRegion.get.offerPatch(since)
          CompleteStream.fromByteBuffer(buf)
        }
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

    private def receiveWithStreamInternal(extraInput: ByteBuffer, ctx: ReceiveContext): PartialFunction[Any, Unit] = {
      case CreateFileRequest(totalLength: Long, crc32: Long) =>
        //primary region
        if (globalSetting.enableCrc && CrcUtils.computeCrc32(extraInput.duplicate()) != crc32) {
          throw new ReceivedMismatchedStreamException();
        }

        val (region: Region, neighbourRegions: Array[RegionInfo]) = chooseRegionForWrite()
        val regionId = region.regionId;

        val maybeLocalId = region.revision
        val mutex = zookeeper.createRegionWriteLock(regionId)
        mutex.acquire()

        try {
          //i am a primary region
          if (globalSetting.replicaNum > 1 &&
            globalSetting.consistencyStrategy == Constants.CONSISTENCY_STRATEGY_STRONG) {

            //create secondary files
            val futures =
              neighbourRegions.map(x => clientOf(x.nodeId).createSecondaryFile(regionId, maybeLocalId, totalLength, crc32,
                extraInput.duplicate()))

            val localId = region.write(extraInput.duplicate(), crc32)

            val results =
              futures.map(Await.result(_, Duration.Inf)).map(x => x.region)

            remoteRegionWatcher.cacheRemoteSeconaryRegions(results)

            ctx.reply(CreateFileResponse(FileId.make(regionId, localId),
              (Set(region.info) ++ results).toArray))
          }
          else {
            val localId = region.write(extraInput.duplicate(), crc32)
            ctx.reply(CreateFileResponse(FileId.make(regionId, localId), Array(region.info)))
          }
        }
        finally {
          if (mutex.isAcquiredInThisProcess) {
            mutex.release()
          }
        }

      case CreateSecondaryFileRequest(regionId: Long, localIdExpected: Long, totalLength: Long, crc32: Long) => {
        if (globalSetting.enableCrc && CrcUtils.computeCrc32(extraInput.duplicate()) != crc32) {
          throw new ReceivedMismatchedStreamException();
        }

        val region = localRegionManager.get(regionId).get
        val localId = region.write(extraInput.duplicate(), crc32)

        ctx.reply(CreateSecondaryFileResponse(FileId.make(regionId, localId), region.info))
      }
    }
  }

}

class RegionFsServerException(msg: String, cause: Throwable = null) extends
  RegionFsException(msg, cause) {
}

class InsufficientNodeServerException(actual: Int, required: Int) extends
  RegionFsServerException(s"insufficient node server for replica: actual: ${actual}, required: ${required}") {

}

class RegionStoreLockedException(storeDir: File, pid: Int) extends
  RegionFsServerException(s"store is locked by another node server: node server pid=${pid}, storeDir=${storeDir.getPath}") {

}

class StoreDirectoryNotExistsException(storeDir: File) extends
  RegionFsServerException(s"store dir does not exist: ${storeDir.getPath}") {

}

class WrongRegionIdException(nodeId: Int, regionId: Long) extends
  RegionFsServerException(s"region not exist on node-$nodeId: ${regionId}") {

}

class ReceivedMismatchedStreamException extends
  RegionFsServerException(s"mismatched checksum exception on receive time") {

}

class FileNotFoundException(nodeId: Int, fileId: FileId) extends
  RegionFsServerException(s"file not found on node-$nodeId: ${fileId}") {

}