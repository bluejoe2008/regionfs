package org.grapheco.regionfs.client

import java.io.InputStream
import java.nio.ByteBuffer
import java.util.concurrent.Executors

import io.netty.buffer.{ByteBuf, Unpooled}
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.{HippoEndpointRef, HippoRpcEnv, HippoRpcEnvFactory}
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import net.neoremind.kraps.util.ByteBufferInputStream
import org.grapheco.commons.util.Logging
import org.grapheco.hippo.util.ByteBufferUtils._
import org.grapheco.regionfs._
import org.grapheco.regionfs.server.{RegionFileEntry, RegionInfo}
import org.grapheco.regionfs.util._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * a client to regionfs servers
  */
class FsClient(zks: String) extends Logging {
  val zookeeper = ZooKeeperClient.create(zks)
  val globalSetting = zookeeper.loadGlobalSetting()
  val clientFactory = new FsNodeClientFactory(globalSetting)
  val ringNodes = new Ring[Int]()
  implicit val ec = clientFactory.executionContext

  //get all nodes
  val cachedClients = mutable.Map[Int, FsNodeClient]()
  val mapNodeWithAddress = mutable.Map[Int, RpcAddress]()
  val nodesWatcher = zookeeper.watchNodeList(
    new ParsedChildNodeEventHandler[NodeServerInfo] {
      override def onCreated(t: NodeServerInfo): Unit = {
        mapNodeWithAddress.synchronized {
          mapNodeWithAddress += t.nodeId -> t.address
        }
        ringNodes.synchronized {
          ringNodes += t.nodeId
        }
      }

      def onUpdated(t: NodeServerInfo): Unit = {
      }

      def onInitialized(batch: Iterable[NodeServerInfo]): Unit = {
        this.synchronized {
          mapNodeWithAddress ++= batch.map(t => t.nodeId -> t.address)
          ringNodes ++= batch.map(_.nodeId)
        }
      }

      override def onDeleted(t: NodeServerInfo): Unit = {
        mapNodeWithAddress.synchronized {
          mapNodeWithAddress -= t.nodeId
        }
        ringNodes.synchronized {
          ringNodes -= t.nodeId
        }
      }

      override def accepts(t: NodeServerInfo): Boolean = true
    })

  //get all regions
  //32768->(1,2), 32769->(1), ...
  private val cachedRegionMap = mutable.Map[Long, Array[RegionInfo]]()

  def clientOf(nodeId: Int): FsNodeClient = {
    cachedClients.getOrElseUpdate(nodeId,
      clientFactory.of(mapNodeWithAddress(nodeId)))
  }

  private def assertNodesNotEmpty(): Unit = {
    if (mapNodeWithAddress.isEmpty) {
      throw new RegionFsClientException("no serving data nodes")
    }
  }

  def writeFile(content: ByteBuffer): Future[FileId] = writeFiles(Array(content)).map(_.head)

  def writeFiles(contents: Array[ByteBuffer]): Future[Array[FileId]] = {
    assertNodesNotEmpty()

    //group files
    val groups = ArrayBuffer[ArrayBuffer[(ByteBuffer, Long)]]()
    var currentGroupSize = 0
    var currentGroup: ArrayBuffer[(ByteBuffer, Long)] = null

    contents.foreach { content =>
      val crc32 = CrcUtils.computeCrc32(content.duplicate())
      currentGroupSize += content.remaining()
      if (currentGroup == null) {
        currentGroup = ArrayBuffer[(ByteBuffer, Long)]()
        groups += currentGroup
      }

      currentGroup += content -> crc32
      if (currentGroupSize >= globalSetting.maxWriteFileBatchSize) {
        currentGroup = null
        currentGroupSize = 0
      }
    }

    val futures: Iterable[Future[Array[FileId]]] = groups.map { group =>
      val chosenNodeId: Int = ringNodes.take()
      val client = clientOf(chosenNodeId)
      client.createFiles(group.toArray).map { x =>
        cacheAliveRegions(x.regionId, x.infos)
        x.fileIds
      }
    }

    Future {
      futures.flatMap(future => Await.result(future, Duration.Inf)).toArray
    }
  }

  def readFile[T](fileId: FileId, consume: (InputStream) => T)(implicit m: Manifest[T]): Future[T] = {
    assertNodesNotEmpty()

    val chosenNodeId = if (globalSetting.replicaNum > 1) {
      val maybeRegionOwnerNodes = cachedRegionMap.get(fileId.regionId).map(
        _.filter(_.revision >= fileId.localId).map(_.nodeId))

      if (maybeRegionOwnerNodes.isEmpty) {
        val nodeId = (fileId.regionId >> 16).toInt
        if (!mapNodeWithAddress.contains(nodeId))
          throw new NoSuitableNodeServerException(fileId)

        nodeId
      }
      else {
        val regionOwnerNodes = maybeRegionOwnerNodes.get
        val maybeNodeId = ringNodes.take(regionOwnerNodes.contains(_))

        if (maybeNodeId.isEmpty)
          throw new NoSuitableNodeServerException(fileId)

        maybeNodeId.get
      }
    }
    else {
      val nodeId = (fileId.regionId >> 16).toInt
      if (!mapNodeWithAddress.contains(nodeId))
        throw new NoSuitableNodeServerException(fileId)

      nodeId
    }

    if (logger.isTraceEnabled()) {
      logger.trace(s"chosen node-$chosenNodeId to read")
    }

    clientOf(chosenNodeId).readFile(
      fileId,
      (head: ReadFileResponseHead, body: ByteBuffer) => {
        if (chosenNodeId == (fileId.regionId >> 16)) {
          cacheAliveRegions(fileId.regionId, head.infos)
        }

        val is = new ByteBufferInputStream(body)
        val t = consume(is)
        is.close()
        t
      }
    )
  }

  private def cacheAliveRegions(regionId: Long, regions: Array[RegionInfo]): Unit = {
    if (!regions.isEmpty)
      cachedRegionMap(regionId) = regions
  }

  def processFiles[X, Y](map: (Iterable[RegionFileEntry]) => X, reduce: (Iterable[X]) => Y): Future[Y] = {
    val futures = mapNodeWithAddress.map(x => clientOf(x._1).processFiles(map))
    Future {
      reduce(futures.map(Await.result(_, Duration.Inf)))
    }
  }

  def deleteFile[T](fileId: FileId): Future[Boolean] = {
    //primary node
    val client = clientOf((fileId.regionId >> 16).toInt)
    val future = client.deleteFile(fileId)

    future.map { x =>
      cacheAliveRegions(fileId.regionId, x.infos)
      x.success
    }
  }

  def close() = {
    nodesWatcher.close()
    clientFactory.close()
    zookeeper.close()
  }
}

/**
  * FsNodeClient factory
  */
class FsNodeClientFactory(globalSetting: GlobalSetting) {
  val refs = mutable.Map[RpcAddress, HippoEndpointRef]()

  val pool = Executors.newFixedThreadPool(globalSetting.executorThreadPoolSize)
  val executionContext: ExecutionContext = ExecutionContext.fromExecutor(pool)

  val rpcEnv: HippoRpcEnv = {
    val rpcConf = new RpcConf()
    val config = RpcEnvClientConfig(rpcConf, "regionfs-client")
    HippoRpcEnvFactory.create(config)
  }

  def of(remoteAddress: RpcAddress): FsNodeClient = {
    val endPointRef = rpcEnv.synchronized {
      refs.getOrElseUpdate(remoteAddress,
        rpcEnv.setupEndpointRef(RpcAddress(remoteAddress.host, remoteAddress.port), "regionfs-service"))
    }

    new FsNodeClient(globalSetting, endPointRef, remoteAddress, executionContext)
  }

  def of(remoteAddress: String): FsNodeClient = {
    val pair = remoteAddress.split(":")
    of(RpcAddress(pair(0), pair(1).toInt))
  }

  def close(): Unit = {
    refs.foreach(x => rpcEnv.stop(x._2))
    refs.clear()
    rpcEnv.shutdown()
    pool.shutdown()
  }
}

/**
  * an FsNodeClient is an underline client used by FsClient
  * it sends raw messages (e.g. SendCompleteFileRequest) to NodeServer and handles responses
  */
class FsNodeClient(globalSetting: GlobalSetting, val endPointRef: HippoEndpointRef, val remoteAddress: RpcAddress, implicit val executionContext: ExecutionContext)
  extends Logging {

  private def safeCall[T](body: => T): T = {
    try {
      body
    }
    catch {
      case e: Throwable => throw new ServerSideException(e)
    }
  }

  def registerSeconaryRegions(localSecondaryRegions: Array[RegionInfo]) = {
    endPointRef.send(RegisterSecondaryRegionsRequest(localSecondaryRegions))
  }

  def createFiles(files: Array[(ByteBuffer, Long)]): Future[CreateFilesResponse] = {
    val buf = Unpooled.buffer(1024)
    for ((filebuf, crc32) <- files) {
      buf.writeLong(filebuf.remaining()).writeLong(crc32)
      buf.writeBytes(filebuf)
    }

    safeCall {
      endPointRef.askWithBuffer[CreateFilesResponse](
        CreateFilesRequest(files.length), buf
      )
    }
  }

  def createSecondaryRegion(regionId: Long): Future[CreateSecondaryRegionResponse] = {
    endPointRef.ask[CreateSecondaryRegionResponse](
      CreateSecondaryRegionRequest(regionId))
  }

  def createSecondaryFiles(
                            regionId: Long,
                            localIds: Array[Long],
                            creationTime: Long,
                            files: Array[(Long, Long, ByteBuf)]): Future[CreateSecondaryFilesResponse] = {
    val buf = Unpooled.buffer(1024)

    for ((localId, (length, crc32, filebuf)) <- localIds.zip(files)) {
      buf.writeLong(localId).writeLong(length).writeLong(crc32)
      buf.writeBytes(filebuf.duplicate())
    }

    safeCall {
      endPointRef.askWithBuffer[CreateSecondaryFilesResponse](
        CreateSecondaryFilesRequest(regionId, creationTime, localIds.length), buf)
    }
  }

  def markSecondaryFilesWritten(regionId: Long, localIds: Array[Long]): Future[MarkSecondaryFilesWrittenResponse] = {
    safeCall {
      endPointRef.askWithBuffer[MarkSecondaryFilesWrittenResponse](
        MarkSecondaryFilesWrittenRequest(regionId, localIds))
    }
  }

  def deleteFile(fileId: FileId): Future[DeleteFileResponse] = {
    safeCall {
      endPointRef.ask[DeleteFileResponse](DeleteFileRequest(fileId))
    }
  }

  def processFiles[T](process: (Iterable[RegionFileEntry]) => T): Future[T] = {
    safeCall {
      endPointRef.ask[ProcessFilesResponse[T]](ProcessFilesRequest(process)).map(_.value)
    }
  }

  def readFile[T](fileId: FileId, consume: (ReadFileResponseHead, ByteBuffer) => T)(implicit m: Manifest[T]): Future[T] = {
    safeCall {
      endPointRef.ask(ReadFileRequest(fileId), (buf: ByteBuffer) => {
        val head = buf.readObject().asInstanceOf[ReadFileResponseHead]
        val body = buf.duplicate()
        val crc32 = CrcUtils.computeCrc32(body.duplicate())
        if (crc32 != head.crc32) {
          throw new WrongFileStreamException(fileId)
        }

        consume(head, body)
      })
    }
  }

  def getPatch[T](regionId: Long, revision: Long, consume: (ByteBuffer) => T)(implicit m: Manifest[T]): Future[T] = {
    safeCall {
      endPointRef.ask(GetRegionPatchRequest(regionId, revision), (buf: ByteBuffer) => {
        consume(buf)
      })
    }
  }

  def getRegionInfos(regionIds: Array[Long]): Future[Array[RegionInfo]] = {
    safeCall {
      endPointRef.ask[GetRegionInfoResponse](
        GetRegionInfoRequest(regionIds)).map(_.infos)
    }
  }
}

class RegionFsClientException(msg: String, cause: Throwable = null)
  extends RegionFsException(msg, cause) {

}

class NoSuitableNodeServerException(fileId: FileId) extends
  RegionFsClientException(s"wrong fileid: $fileId") {

}

class WrongFileStreamException(fileId: FileId) extends
  RegionFsClientException(s"wrong stream of file: $fileId") {

}

class ServerSideException(cause: Throwable) extends
  RegionFsClientException(s"got a server side error: ${cause.getMessage}") {

}