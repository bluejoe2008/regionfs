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
import org.grapheco.hippo.util.ByteBufferUtils._
import org.grapheco.hippo.{ChunkedStream, HippoRpcHandler, PooledMessageStream, ReceiveContext}
import org.grapheco.regionfs._
import org.grapheco.regionfs.client._
import org.grapheco.regionfs.util._

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

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
      throw new StoreDirectoryNotFoundException(storeDir)

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
class FsNodeServer(val zks: String, val nodeId: Int, val storeDir: File, host: String, port: Int) extends Logging {
  assert(nodeId > 0)

  if (logger.isDebugEnabled())
    logger.debug(s"[node-$nodeId] initializing, storeDir: ${storeDir.getCanonicalFile.getAbsolutePath}")

  val zookeeper = ZooKeeperClient.create(zks)
  val (env, address) = createRpcEnv(zookeeper)

  val globalSetting = zookeeper.loadGlobalSetting()

  val localRegionManager = new LocalRegionManager(
    nodeId,
    storeDir,
    globalSetting,
    new RegionEventListener {
      override def handleRegionEvent(event: RegionEvent): Unit = {
      }
    })

  val clientFactory = new FsNodeClientFactory(globalSetting)

  //get neighbour nodes
  val cachedClients = mutable.Map[Int, FsNodeClient]()
  val mapNeighbourNodeWithAddress = mutable.Map[Int, RpcAddress]()
  val mapNeighbourNodeWithRegionCount = mutable.Map[Int, Int]()
  val zkNodeEventHandlers = new CompositeParsedChildNodeEventHandler[NodeServerInfo]()

  zkNodeEventHandlers.addHandler(
    new ParsedChildNodeEventHandler[NodeServerInfo] {
      override def accepts(t: NodeServerInfo): Boolean = true

      override def onUpdated(t: NodeServerInfo): Unit = {
      }

      override def onCreated(t: NodeServerInfo): Unit = {
      }

      override def onInitialized(batch: Iterable[NodeServerInfo]): Unit = {
        batch.find(_.nodeId == nodeId).foreach { x =>
          throw new NodeIdAlreadyExistException(x)
        }
      }

      override def onDeleted(t: NodeServerInfo): Unit = {
      }
    }).addHandler(
    new ParsedChildNodeEventHandler[NodeServerInfo] {
      override def onCreated(t: NodeServerInfo): Unit = {
        mapNeighbourNodeWithAddress += t.nodeId -> t.address
        mapNeighbourNodeWithRegionCount += t.nodeId -> t.regionCount
      }

      def onUpdated(t: NodeServerInfo): Unit = {
        mapNeighbourNodeWithRegionCount += t.nodeId -> t.regionCount
      }

      def onInitialized(batch: Iterable[NodeServerInfo]): Unit = {
        mapNeighbourNodeWithAddress.synchronized {
          mapNeighbourNodeWithAddress ++= batch.map(t => t.nodeId -> t.address)
          mapNeighbourNodeWithRegionCount ++= batch.map(t => t.nodeId -> t.regionCount)
        }
      }

      override def onDeleted(t: NodeServerInfo): Unit = {
        mapNeighbourNodeWithAddress -= t.nodeId
        mapNeighbourNodeWithRegionCount -= t.nodeId
      }

      override def accepts(t: NodeServerInfo): Boolean = t.nodeId != nodeId
    })

  var alive: Boolean = true

  val remoteRegionWatcher: RemoteRegionWatcher = new RemoteRegionWatcher(nodeId,
    globalSetting, zkNodeEventHandlers, localRegionManager, clientOf)

  val neighbourNodesWatcher = zookeeper.watchNodeList(zkNodeEventHandlers)

  val endpoint = new FileRpcEndpoint(env)
  env.setupEndpoint("regionfs-service", endpoint)
  env.setRpcHandler(endpoint)
  writeLockFile(new File(storeDir, ".lock"))

  def awaitTermination(): Unit = {
    println(IOUtils.toString(this.getClass.getClassLoader.getResourceAsStream("logo.txt"), "utf-8"))
    println(s"[node-$nodeId] starting node server on $address, storeDir=${storeDir.getAbsoluteFile.getCanonicalPath}")

    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run(): Unit = {
        shutdown()
      }
    })

    env.awaitTermination()
  }

  def shutdown(): Unit = {
    if (alive) {
      clientFactory.close()
      neighbourNodesWatcher.close
      remoteRegionWatcher.close()

      new File(storeDir, ".lock").delete()
      env.shutdown()
      zookeeper.close()
      println(s"[node-$nodeId] shutdown node server on $address")
      alive = false
    }
  }

  private def clientOf(nodeId: Int): FsNodeClient = {
    cachedClients.synchronized {
      cachedClients.getOrElseUpdate(nodeId,
        clientFactory.of(mapNeighbourNodeWithAddress(nodeId)))
    }
  }

  def cleanData(): Unit = {
    throw new NotImplementedError()
  }

  private def writeLockFile(lockFile: File): Unit = {
    val pid = ProcessUtils.getCurrentPid()
    val fos = new FileOutputStream(lockFile)
    fos.write(pid.toString.getBytes())
    fos.close()
  }

  private def createRpcEnv(zookeeper: ZooKeeperClient): (HippoRpcEnv, RpcAddress) = {
    val env = HippoRpcEnvFactory.create(
      RpcEnvServerConfig(new RpcConf(), "regionfs-server", host, port))

    val address = env.address
    env -> address
  }

  class FileRpcEndpoint(override val rpcEnv: HippoRpcEnv)
    extends RpcEndpoint
      with HippoRpcHandler
      with Logging {

    Unpooled.buffer(1024)

    private val traffic = new AtomicInteger(0)

    //NOTE: register only on started up
    override def onStart(): Unit = {
      //register this node
      zookeeper.createNodeNode(nodeId, address, localRegionManager)
    }

    private def createNewRegion(): (Region, Array[RegionInfo]) = {
      val localRegion = localRegionManager.createNew()
      val regionId = localRegion.regionId

      val neighbourRegions: Array[RegionInfo] = {
        if (globalSetting.replicaNum <= 1) {
          Array()
        }
        else {
          if (mapNeighbourNodeWithAddress.size < globalSetting.replicaNum - 1)
            throw new InsufficientNodeServerException(mapNeighbourNodeWithAddress.size, globalSetting.replicaNum - 1)

          //sort neighbour nodes by region count
          val thinNodeIds = mapNeighbourNodeWithRegionCount.toArray.sortBy(_._2).take(globalSetting.replicaNum - 1).map(_._1)
          val futures = thinNodeIds.map(clientOf(_).createSecondaryRegion(regionId))

          //hello, pls create a new localRegion with id=regionId
          val neighbourResults = futures.map(Await.result(_, Duration.Inf).info)
          zookeeper.updateNodeData(nodeId, address, localRegionManager)
          remoteRegionWatcher.cacheRemoteSeconaryRegions(neighbourResults)
          neighbourResults
        }
      }

      localRegion -> neighbourRegions
    }

    private def chooseRegionForWrite(): (Region, Array[RegionInfo]) = {
      val writableRegions = localRegionManager.regions.values.toArray.filter(x => x.isWritable && x.isPrimary)

      //too few writable regions
      if (writableRegions.length < globalSetting.minWritableRegions) {
        createNewRegion()
      }
      else {
        val localRegion = localRegionManager.synchronized {
          localRegionManager.regions(localRegionManager.ring.take(x =>
            writableRegions.exists(_.regionId == x)).get)
        }

        localRegion -> remoteRegionWatcher.getSecondaryRegions(localRegion.regionId)
      }
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case x: Any =>
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

    override def onStop(): Unit = {
      logger.info("stop endpoint")
    }

    override def openChunkedStream(): PartialFunction[Any, ChunkedStream] = {
      case x: Any =>
        traffic.incrementAndGet()
        val t = openChunkedStreamInternal.apply(x)
        traffic.decrementAndGet()
        t
    }

    override def receiveWithBuffer(extraInput: ByteBuffer, context: ReceiveContext): PartialFunction[Any, Unit] = {
      case x: Any =>
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

    private def receiveAndReplyInternal(ctx: RpcCallContext): PartialFunction[Any, Unit] = {
      //TODO: cancelable task
      case ProcessFilesRequest(process) => {
        val results = process(localRegionManager.regions.values.filter(_.isPrimary).flatMap {
          region =>
            region.listFiles()
        })

        ctx.reply(ProcessFilesResponse(results))
      }

      case GetRegionOwnerNodesRequest(regionId: Long) =>
        ctx.reply(GetRegionOwnerNodesResponse(localRegionManager.regions.get(regionId).map(_.info).toArray
          ++ remoteRegionWatcher.cachedRemoteSecondaryRegions(regionId)))

      case GetRegionsOnNodeRequest() =>
        ctx.reply(GetRegionsOnNodeResponse(localRegionManager.regions.values.map(_.info).toArray))

      case GetNodeStatRequest() =>
        val nodeStat = NodeStat(nodeId, address,
          localRegionManager.regions.values.filter(_.isPrimary).map { region =>
            RegionStat(region.regionId, region.fileCount, region.bodyLength)
          }.toList)

        ctx.reply(GetNodeStatResponse(nodeStat))

      case GetRegionInfoRequest(regionIds: Array[Long]) =>
        val infos = regionIds.map(regionId => {
          val region = localRegionManager.regions(regionId)
          region.info
        })

        ctx.reply(GetRegionInfoResponse(infos))

      //create region as replica
      case CreateSecondaryRegionRequest(regionId: Long) =>
        val region = localRegionManager.createSecondaryRegion(regionId)
        zookeeper.updateNodeData(nodeId, address, localRegionManager)
        ctx.reply(CreateSecondaryRegionResponse(region.info))

      case ShutdownRequest() =>
        ctx.reply(ShutdownResponse(address))
        shutdown()

      case CleanDataRequest() =>
        cleanData()
        ctx.reply(CleanDataResponse(address))

      case RegisterSecondaryRegionsRequest(regions: Array[RegionInfo]) =>
        remoteRegionWatcher.cacheRemoteSeconaryRegions(regions)

      case DeleteSecondaryFileRequest(fileId: FileId) =>
        handleDeleteSeconaryFileRequest(fileId, ctx)

      case DeleteFileRequest(fileId: FileId) =>
        handleDeleteFileRequest(fileId, ctx)

      case GreetingRequest(msg: String) =>
        println(s"node-$nodeId($address): \u001b[31;47;4m${msg}\u0007\u001b[0m")
        ctx.reply(GreetingResponse(address))
    }

    private def handleDeleteFileRequest(fileId: FileId, ctx: RpcCallContext): Unit = {
      val maybeRegion = localRegionManager.get(fileId.regionId)
      if (maybeRegion.isEmpty) {
        throw new RegionNotFoundOnNodeServerException(nodeId, fileId.regionId)
      }

      try {
        val localRegion: Region = maybeRegion.get
        if (localRegion.revision <= fileId.localId)
          throw new FileNotFoundException(nodeId, fileId)

        val success = localRegion.delete(fileId.localId)
        val regions =
        //is a primary region?
          if (globalSetting.replicaNum > 1 && (fileId.regionId >> 16) == nodeId) {
            //notify secondary regions
            val futures = remoteRegionWatcher.getSecondaryRegions(fileId.regionId).map(x =>
              clientOf(x.nodeId).endPointRef.ask[DeleteSecondaryFileResponse](
                DeleteSecondaryFileRequest(fileId)))

            //TODO: if fail?
            (Set(localRegion.info) ++ futures.map(Await.result(_, Duration.Inf).info)).toArray
          }
          else {
            Array(localRegion.info)
          }

        remoteRegionWatcher.cacheRemoteSeconaryRegions(regions)
        ctx.reply(DeleteFileResponse(success, null, regions))
      }
      catch {
        case e: Throwable =>
          ctx.reply(DeleteFileResponse(false, e.getMessage, Array()))
      }
    }

    private def handleDeleteSeconaryFileRequest(fileId: FileId, ctx: RpcCallContext): Unit = {
      val maybeRegion = localRegionManager.get(fileId.regionId)
      if (maybeRegion.isEmpty) {
        throw new RegionNotFoundOnNodeServerException(nodeId, fileId.regionId)
      }

      try {
        if (maybeRegion.get.revision <= fileId.localId)
          throw new FileNotFoundException(nodeId, fileId)

        val success = maybeRegion.get.delete(fileId.localId)
        ctx.reply(DeleteSecondaryFileResponse(success, null, maybeRegion.get.info))
      }
      catch {
        case e: Throwable =>
          ctx.reply(DeleteSecondaryFileResponse(false, e.getMessage, maybeRegion.get.info))
      }
    }

    private def openChunkedStreamInternal(): PartialFunction[Any, ChunkedStream] = {
      case ListFileRequest() =>
        handleListFileRequest()
    }

    private def handleListFileRequest(): PooledMessageStream[ListFileResponseDetail] = {
      ChunkedStream.pooled[ListFileResponseDetail](1024, (pool) => {
        localRegionManager.regions.values.filter(_.isPrimary).foreach { x =>
          val it = x.listFiles().map(entry => entry.id -> entry.length)
          it.foreach(x => pool.push(ListFileResponseDetail(x)))
        }
      })
    }

    private def receiveWithStreamInternal(extraInput: ByteBuffer, ctx: ReceiveContext): PartialFunction[Any, Unit] = {
      case GetRegionPatchRequest(regionId: Long, since: Long) =>
        handleGetRegionPatchRequest(regionId, since, ctx)

      case ReadFileRequest(fileId: FileId) =>
        handleReadFileRequest(fileId, ctx)

      case CreateFileRequest(totalLength: Long, crc32: Long) =>
        handleCreateFileRequest(totalLength, crc32, extraInput, ctx)

      case CreateSecondaryFileRequest(regionId: Long, localId: Long, totalLength: Long, crc32: Long) =>
        handleCreateSecondaryFileRequest(regionId, localId, totalLength, crc32, extraInput, ctx)

      case MarkSecondaryFileWrittenRequest(regionId: Long, localId: Long, totalLength: Long) =>
        handleMarkSecondaryFileWrittenRequest(regionId, localId, totalLength, ctx)
    }

    private def handleGetRegionPatchRequest(regionId: Long, since: Long, ctx: ReceiveContext): Unit = {
      val maybeRegion = localRegionManager.get(regionId)
      if (maybeRegion.isEmpty) {
        throw new RegionNotFoundOnNodeServerException(nodeId, regionId)
      }

      if (traffic.get() > Constants.MAX_BUSY_TRAFFIC) {
        ctx.replyBuffer(Unpooled.buffer(1024).writeByte(Constants.MARK_GET_REGION_PATCH_SERVER_IS_BUSY))
      }
      else {
        ctx.replyBuffer(maybeRegion.get.offerPatch(since))
      }
    }

    private def handleReadFileRequest(fileId: FileId, ctx: ReceiveContext): Unit = {
      val maybeRegion = localRegionManager.get(fileId.regionId)

      if (maybeRegion.isEmpty) {
        throw new RegionNotFoundOnNodeServerException(nodeId, fileId.regionId)
      }

      val localRegion: Region = maybeRegion.get
      val maybeBuffer = localRegion.read(fileId.localId)
      if (maybeBuffer.isEmpty) {
        throw new FileNotFoundException(nodeId, fileId)
      }

      val body = Unpooled.wrappedBuffer(maybeBuffer.get.duplicate())
      val crc32 = CrcUtils.computeCrc32(maybeBuffer.get.duplicate())

      val buf = Unpooled.buffer()
      buf.writeObject(ReadFileResponseHead(body.readableBytes(), crc32,
        (Set(localRegion.info) ++ remoteRegionWatcher.getSecondaryRegions(fileId.regionId)).toArray))

      buf.writeBytes(body)
      ctx.replyBuffer(buf)
    }

    private def handleMarkSecondaryFileWrittenRequest(regionId: Long, localId: Long, totalLength: Long, ctx: ReceiveContext): Unit = {
      assert(totalLength >= 0)
      val region = localRegionManager.get(regionId).get
      region.markGlobalWritten(localId, totalLength)

      ctx.reply(MarkSecondaryFileWrittenResponse(regionId, localId, region.info))
    }

    private def handleCreateSecondaryFileRequest(regionId: Long, localId: Long, totalLength: Long, crc32: Long, extraInput: ByteBuffer, ctx: ReceiveContext): Unit = {
      assert(totalLength >= 0)

      if (CrcUtils.computeCrc32(extraInput.duplicate()) != crc32) {
        throw new ReceivedMismatchedStreamException()
      }

      val region = localRegionManager.get(regionId).get
      region.writeLogFile(localId, extraInput.duplicate(), crc32)
      region.markLocalWriten(localId)

      ctx.reply(CreateSecondaryFileResponse(regionId, localId))
    }

    private def handleCreateFileRequest(totalLength: Long, crc32: Long, extraInput: ByteBuffer, ctx: ReceiveContext): Unit = {
      //primary region
      if (CrcUtils.computeCrc32(extraInput.duplicate()) != crc32) {
        throw new ReceivedMismatchedStreamException()
      }

      val (localRegion: Region, neighbourRegions: Array[RegionInfo]) = chooseRegionForWrite()
      val regionId = localRegion.regionId

      val mutex = zookeeper.createRegionWriteLock(regionId)
      mutex.acquire()

      try {
        //yes! i am a primary region
        if (globalSetting.replicaNum > 1 &&
          globalSetting.consistencyStrategy == Constants.CONSISTENCY_STRATEGY_STRONG) {

          val tx = Atomic("create local id") {
            case _ =>
              localRegion.createLocalId()
          } --> Atomic("request to create secondary file") {
            case localId: Long =>
              Rollbackable.success(localId -> neighbourRegions.map(x =>
                clientOf(x.nodeId).createSecondaryFile(regionId, localId, totalLength, crc32,
                  extraInput.duplicate()))) {}
          } --> Atomic("save region log & mem") {
            case (localId: Long, futures: Array[Future[CreateSecondaryFileResponse]]) =>
              localRegion.writeLogFile(localId, extraInput.duplicate(), crc32) map {
                case _ =>
                  Rollbackable.success(localId -> futures) {}
              }
          } --> Atomic("waits all secondary file creation response") {
            case (localId: Long, futures: Array[Future[CreateSecondaryFileResponse]]) =>
              futures.foreach(Await.result(_, Duration.Inf))

              Rollbackable.success(localId) {}
          } --> Atomic("mark local written") {
            case (localId: Long) =>
              localRegion.markLocalWriten(localId)
          } --> Atomic("mark global written") {
            case (localId: Long) =>
              val futures =
                neighbourRegions.map(x => clientOf(x.nodeId).markSecondaryFileWritten(regionId, localId, totalLength))

              val neighbourResults = futures.map(Await.result(_, Duration.Inf)).map(x => x.info)

              localRegion.markGlobalWritten(localId, totalLength)
              remoteRegionWatcher.cacheRemoteSeconaryRegions(neighbourResults)

              val fid = FileId.make(regionId, localId)
              ctx.reply(CreateFileResponse(fid, (Set(localRegion.info) ++ neighbourResults).toArray))

              Rollbackable.success(fid) {
              }
          }

          TransactionRunner.perform(tx, regionId, RetryStrategy.FOR_TIMES(globalSetting.maxWriteRetryTimes))
        }
        else {
          val tx = Atomic("create local id") {
            case _ =>
              localRegion.createLocalId()
          } --> Atomic("save local file") {
            case localId: Long =>
              localRegion.writeLogFile(localId, extraInput.duplicate(), crc32)
          } --> Atomic("mark global written") {
            case (localId: Long) =>
              localRegion.markGlobalWritten(localId, totalLength)
          } --> Atomic("response") {
            case localId: Long =>
              val fid = FileId.make(regionId, localId)

              ctx.reply(CreateFileResponse(fid, Array(localRegion.info)))
              Rollbackable.success(fid) {}
          }

          TransactionRunner.perform(tx, regionId, RetryStrategy.FOR_TIMES(globalSetting.maxWriteRetryTimes))
        }
      }
      finally {
        if (mutex.isAcquiredInThisProcess) {
          mutex.release()
        }
      }
    }
  }

}

class RegionFsServerException(msg: String, cause: Throwable = null) extends
  RegionFsException(msg, cause) {
}

class InsufficientNodeServerException(actual: Int, required: Int) extends
  RegionFsServerException(s"insufficient node server for replica, actual: $actual, required: $required") {

}

class RegionStoreLockedException(storeDir: File, pid: Int) extends
  RegionFsServerException(s"store is locked by another node server: node server pid=$pid, storeDir=${storeDir.getPath}") {

}

class StoreDirectoryNotFoundException(storeDir: File) extends
  RegionFsServerException(s"store dir does not exist: ${storeDir.getPath}") {

}

class RegionNotFoundOnNodeServerException(nodeId: Int, regionId: Long) extends
  RegionFsServerException(s"region not found on node-$nodeId: $regionId") {

}

class ReceivedMismatchedStreamException extends
  RegionFsServerException(s"mismatched checksum exception on receive time") {

}

class FileNotFoundException(nodeId: Int, fileId: FileId) extends
  RegionFsServerException(s"file not found on node-$nodeId: $fileId") {

}

class NodeIdAlreadyExistException(nodeServerInfo: NodeServerInfo) extends
  RegionFsServerException(s"node id already exist: $nodeServerInfo") {

}