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
import org.grapheco.regionfs.util.{CrcUtils, ParsedChildNodeEventHandler, RegionFsException, ZooKeeperClient}

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
    val storeDir = conf.get("data.storeDir").asFile(baseDir).
      getCanonicalFile.getAbsoluteFile

    if (!storeDir.exists())
      throw new StoreDirNotExistsException(storeDir)

    val lockFile = new File(storeDir, ".lock")
    if (lockFile.exists()) {
      val fis = new FileInputStream(lockFile)
      val pid = IOUtils.toString(fis).toInt
      fis.close()

      throw new RegionStoreLockedException(storeDir, pid)
    }

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
  val globalSetting = zookeeper.loadGlobalSetting()

  val localRegionManager = new RegionManager(nodeId, storeDir, globalSetting, new RegionEventListener {
    override def handleRegionEvent(event: RegionEvent): Unit = {
      event match {
        case CreateRegionEvent(region) => {
          //registered
        }

        case WriteRegionEvent(region) => {
          if (globalSetting.consistencyStrategy == Constants.CONSISTENCY_STRATEGY_EVENTUAL)
            zookeeper.updateRegionNode(region.nodeId, region)
        }
      }
    }
  })

  val clientFactory = new FsNodeClientFactory(globalSetting);

  //get neighbour nodes
  val cachedClients = mutable.Map[Int, FsNodeClient]()
  val mapNeighbourNodeWithAddress = mutable.Map[Int, RpcAddress]()
  val neighbourNodesWatcher = zookeeper.watchNodeList(
    new ParsedChildNodeEventHandler[(Int, RpcAddress)] {
      override def onCreated(t: (Int, RpcAddress)): Unit = {
        mapNeighbourNodeWithAddress += t
      }

      def onUpdated(t: (Int, RpcAddress)): Unit = {
      }

      def onInitialized(batch: Iterable[(Int, RpcAddress)]): Unit = {
        mapNeighbourNodeWithAddress.synchronized {
          mapNeighbourNodeWithAddress.clear()
          mapNeighbourNodeWithAddress ++= batch
        }
      }

      override def onDeleted(t: (Int, RpcAddress)): Unit = {
        mapNeighbourNodeWithAddress -= t._1
      }

      override def accepts(t: (Int, RpcAddress)): Boolean = {
        nodeId != t._1
      }
    })

  val primaryRegionWatcher: Option[PrimaryRegionWatcher] = {
    if (globalSetting.consistencyStrategy == Constants.CONSISTENCY_STRATEGY_EVENTUAL) {
      Some(new PrimaryRegionWatcher(zookeeper, globalSetting, nodeId, localRegionManager, clientOf(_)).start)
    }
    else {
      None
    }
  }

  //get regions in neighbour nodes
  //TODO: unneccesary for secondary regions?
  //32768->(1,2), 32769->(1), ...
  val mapNeighbourRegionWithNodes = mutable.Map[Long, mutable.Map[Int, Long]]()
  val mapNeighbourNodeWithRegionCount = mutable.ListMap[Int, AtomicInteger]()

  val neighbourRegionsWatcher = zookeeper.watchRegionList(
    new ParsedChildNodeEventHandler[(Long, Int, Long)] {
      override def onCreated(t: (Long, Int, Long)): Unit = {
        this.synchronized {
          mapNeighbourNodeWithRegionCount.getOrElseUpdate(t._2, new AtomicInteger(0)).incrementAndGet()
          mapNeighbourRegionWithNodes.getOrElseUpdate(t._1, mutable.Map[Int, Long]()) += (t._2 -> t._3)
        }
      }

      def onUpdated(t: (Long, Int, Long)): Unit = {
        mapNeighbourRegionWithNodes.synchronized {
          mapNeighbourRegionWithNodes(t._1).update(t._2, t._3)
        }
      }

      def onInitialized(batch: Iterable[(Long, Int, Long)]): Unit = {
        this.synchronized {
          mapNeighbourRegionWithNodes.clear()
          mapNeighbourNodeWithRegionCount.clear()
          mapNeighbourRegionWithNodes ++= batch.groupBy(_._1).map(x =>
            x._1 -> (mutable.Map[Int, Long]() ++ x._2.map(y => y._2 -> y._3)))
          mapNeighbourNodeWithRegionCount ++= batch.groupBy(_._2).map(x => x._1 -> new AtomicInteger(x._2.size))
        }
      }

      override def onDeleted(t: (Long, Int, Long)): Unit = {
        this.synchronized {
          mapNeighbourNodeWithRegionCount(t._2).decrementAndGet()
          mapNeighbourRegionWithNodes(t._1) -= t._2
        }
      }

      override def accepts(t: (Long, Int, Long)): Boolean = {
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
      primaryRegionWatcher.foreach(_.stop())
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
        if (globalSetting.replicaNum <= 1) {
          Array[Int]()
        }
        else {
          if (mapNeighbourNodeWithAddress.size < globalSetting.replicaNum - 1)
            throw new InsufficientNodeServerException(mapNeighbourNodeWithAddress.size, globalSetting.replicaNum);

          //notify neighbours
          //find thinnest neighbour which has least regions

          //TODO: very very time costing
          val thinNodeIds = mapNeighbourNodeWithAddress.map(
            x => x._1 -> mapNeighbourNodeWithRegionCount.getOrElse(x._1, new AtomicInteger(0)).get).
            toList.sortBy(_._2).takeRight(globalSetting.replicaNum - 1).map(_._1)

          val futures = thinNodeIds.map(clientOf(_).createSecondaryRegion(regionId))

          //hello, pls create a new region with id=regionId
          futures.map(Await.result(_, Duration.Inf)).map(_.nodeId).toArray
        }
      }

      //ok, now I register this region
      nodeIds.foreach(zookeeper.createRegionNode(_, region))
      zookeeper.createRegionNode(nodeId, region)

      region -> nodeIds
    }

    private def chooseRegionForWrite(): (Region, Array[Int]) = {
      val writableRegions = localRegionManager.regions.values.toArray.filter(_.isWritable).filter(_.isPrimary);

      //too few writable regions
      if (writableRegions.size < globalSetting.minWritableRegions) {
        createNewRegion()
      }
      else {
        //TODO: ugly code
        //val region = writableRegions.sortBy(_.length).head
        localRegionManager.synchronized {
          val region = localRegionManager.regions(localRegionManager.ring.!(x =>
            writableRegions.find(_.regionId == x).isDefined).get)
          region -> mapNeighbourRegionWithNodes.get(region.regionId).map(_.map(_._1).toArray).getOrElse(Array[Int]())
        }
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
      case GetNodeStatRequest() => {
        val nodeStat = NodeStat(nodeId, address,
          localRegionManager.regions.map { kv =>
            RegionStat(kv._1, kv._2.fileCount, kv._2.length)
          }.toList)

        ctx.reply(GetNodeStatResponse(nodeStat))
      }

      case GetRegionStatusRequest(regionIds: Array[Long]) => {
        val status = regionIds.map(regionId => {
          val region = localRegionManager.regions(regionId)
          region.status
        })

        ctx.reply(GetRegionStatusResponse(status))
      }

      //create region as replica
      case CreateSecondaryRegionRequest(regionId: Long) => {
        localRegionManager.createSecondaryRegion(regionId)
        ctx.reply(CreateSecondaryRegionResponse(regionId, nodeId))
      }

      case ShutdownRequest() => {
        ctx.reply(ShutdownResponse(address))
        shutdown()
      }

      case CleanDataRequest() => {
        cleanData()
        ctx.reply(CleanDataResponse(address))
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
          //is a primary region?
          if (globalSetting.replicaNum > 1 && (fileId.regionId >> 16) == nodeId) {
            //notify secondary regions
            val futures = mapNeighbourRegionWithNodes(fileId.regionId).map(x =>
              clientOf(x._1).endPointRef.ask[DeleteFileResponse](
                DeleteFileRequest(fileId)))

            //TODO: if fail?
            futures.foreach(Await.result(_, Duration.Inf))
          }

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
      case ReadFileRequest(fileId: FileId) => {
        val maybeRegion = localRegionManager.get(fileId.regionId)

        if (maybeRegion.isEmpty) {
          throw new WrongRegionIdException(nodeId, fileId.regionId);
        }

        val maybeBuffer = maybeRegion.get.read(fileId.localId)
        if (maybeBuffer.isEmpty) {
          throw new FileNotFoundException(nodeId, fileId);
        }

        CompleteStream.fromByteBuffer(maybeBuffer.get)
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

        val (region: Region, neighbourNodeIds: Array[Int]) = chooseRegionForWrite()
        val regionId = region.regionId;

        val maybeLocalId = region.revision
        val mutex = zookeeper.createRegionWriteLock(regionId)
        mutex.acquire()

        try {
          //i am a primary region
          if (globalSetting.replicaNum > 1 &&
            globalSetting.consistencyStrategy == Constants.CONSISTENCY_STRATEGY_STRONG) {

            //notify secondary regions
            val futures =
              neighbourNodeIds.map(x => clientOf(x).createSecondaryFile(regionId, maybeLocalId, totalLength, crc32,
                extraInput.duplicate()))

            val (localId, revision) = region.write(extraInput.duplicate(), crc32)

            val arrayNodeWithRevision =
              futures.map(Await.result(_, Duration.Inf)).map(x => x.nodeId -> x.revision)

            ctx.reply(CreateFileResponse(FileId.make(regionId, localId),
              (Set(nodeId -> revision) ++ arrayNodeWithRevision).toArray))
          }
          else {
            val (localId, revision) = region.write(extraInput.duplicate(), crc32)
            ctx.reply(CreateFileResponse(FileId.make(regionId, localId), Array(nodeId -> revision)))
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
        val (localId, revision) = region.write(extraInput.duplicate(), crc32)

        ctx.reply(CreateSecondaryFileResponse(FileId.make(regionId, localId), nodeId, revision))
      }
    }
  }

}

class PrimaryRegionWatcher(zookeeper: ZooKeeperClient,
                           conf: GlobalSetting,
                           nodeId: Int,
                           localRegionManager: RegionManager,
                           clientOf: (Int) => FsNodeClient)
  extends Logging {
  var stopped: Boolean = false;
  val thread: Thread = new Thread(new Runnable() {
    override def run(): Unit = {
      while (!stopped) {
        Thread.sleep(conf.regionVersionCheckInterval)
        if (!stopped) {
          val secondaryRegions = localRegionManager.regions.values.filter(!_.isPrimary).groupBy(x =>
            (x.regionId >> 16).toInt)
          for (x <- secondaryRegions if (!stopped)) {
            try {
              val regionIds = x._2.map(_.regionId).toArray
              val statusList = Await.result(clientOf(x._1).getRegionStatus(regionIds), Duration("2s"))
              statusList.foreach(status => {
                val localRegion = localRegionManager.regions(status.regionId)
                //local is old
                val targetRevision: Long = status.revision
                val localRevision: Long = localRegion.revision
                if (targetRevision > localRevision) {
                  if (logger.isTraceEnabled())
                    logger.trace(s"[region-${localRegion.regionId}@${nodeId}] found new version : ${targetRevision}>${localRevision}");

                  val is = clientOf(x._1).getPatchInputStream(
                    localRegion.regionId, localRevision, Duration("10s"))

                  val updated = localRegion.applyPatch(is);
                  is.close();

                  if (updated) {
                    //FIXME: region.close() will cause current writing fail
                    val updatedRegion = localRegionManager.update(localRegion)
                    if (logger.isTraceEnabled())
                      logger.trace(s"[region-${localRegion.regionId}@${nodeId}] updated: ${localRevision}->${updatedRegion.revision}");
                  }
                }
              })
            }
            catch {
              case t: Throwable =>
                if (logger.isWarnEnabled())
                  logger.warn(t.getMessage)
            }
          }
        }
      }
    }
  })

  def start(): PrimaryRegionWatcher = {
    thread.start();
    this;
  }

  def stop(): Unit = {
    stopped = true;
    thread.join()
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

class StoreDirNotExistsException(storeDir: File) extends
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