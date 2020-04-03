package org.grapheco.regionfs.client

import java.io.InputStream
import java.nio.ByteBuffer
import java.util.concurrent.Executors

import io.netty.buffer.Unpooled
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.{HippoEndpointRef, HippoRpcEnv, HippoRpcEnvFactory}
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import net.neoremind.kraps.util.ByteBufferInputStream
import org.grapheco.commons.util.Logging
import org.grapheco.hippo.util.ByteBufferUtils._
import org.grapheco.regionfs._
import org.grapheco.regionfs.server.{FileEntry, RegionInfo}
import org.grapheco.regionfs.util._

import scala.collection.mutable
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

  def writeFile(content: ByteBuffer): Future[FileId] = {
    assertNodesNotEmpty()
    val crc32 = CrcUtils.computeCrc32(content.duplicate())
    val chosenNodeId: Int = ringNodes.take()
    val client = clientOf(chosenNodeId)
    implicit val ec: ExecutionContext = client.executionContext
    client.createFile(crc32, content.duplicate()).map(
      x => {
        //update local cache
        cacheAliveRegions(x.fileId.regionId, x.infos)
        x.fileId
      })
  }

  def readFile[T](fileId: FileId, consume: (InputStream) => T)(implicit m: Manifest[T]): Future[T] = {
    assertNodesNotEmpty()

    val chosenNodeId = if (globalSetting.replicaNum > 1) {
      val maybeRegionOwnerNodes = cachedRegionMap.get(fileId.regionId).map(
        _.filter(_.revision > fileId.localId).map(_.nodeId))

      if (maybeRegionOwnerNodes.isEmpty) {
        val nodeId = (fileId.regionId >> 16).toInt
        if (!mapNodeWithAddress.contains(nodeId))
          throw new WrongFileIdException(fileId)

        nodeId
      }
      else {
        val regionOwnerNodes = maybeRegionOwnerNodes.get
        val maybeNodeId = ringNodes.take(regionOwnerNodes.contains(_))

        if (maybeNodeId.isEmpty)
          throw new WrongFileIdException(fileId)

        maybeNodeId.get
      }
    }
    else {
      val nodeId = (fileId.regionId >> 16).toInt
      if (!mapNodeWithAddress.contains(nodeId))
        throw new WrongFileIdException(fileId)

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

  def processFiles[X, Y](map: (Iterable[FileEntry]) => X, reduce: (Iterable[X]) => Y): Future[Y] = {
    val futures = mapNodeWithAddress.map(x => clientOf(x._1).processFiles(map))
    implicit val ec: ExecutionContext = clientFactory.executionContext
    Future {
      reduce(futures.map(Await.result(_, Duration.Inf)))
    }
  }

  def deleteFile[T](fileId: FileId): Future[Boolean] = {
    //primary node
    val client = clientOf((fileId.regionId >> 16).toInt)
    val future = client.deleteFile(fileId)
    implicit val ec: ExecutionContext = clientFactory.executionContext

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
      case e: Throwable => throw new ServerRaisedException(e)
    }
  }

  def createFile(crc32: Long, content: ByteBuffer): Future[CreateFileResponse] = {
    safeCall {
      endPointRef.askWithStream[CreateFileResponse](
        CreateFileRequest(content.remaining(), crc32),
        Unpooled.wrappedBuffer(content)
      )
    }
  }

  def registerSeconaryRegions(localSecondaryRegions: Array[RegionInfo]) = {
    endPointRef.send(RegisterSeconaryRegionsRequest(localSecondaryRegions))
  }

  def createSecondaryRegion(regionId: Long): Future[CreateSecondaryRegionResponse] = {
    endPointRef.ask[CreateSecondaryRegionResponse](
      CreateSecondaryRegionRequest(regionId))
  }

  def createSecondaryFile(
                           regionId: Long,
                           maybeLocalId: Long,
                           totalLength: Long,
                           crc32: Long,
                           content: ByteBuffer): Future[CreateSecondaryFileResponse] = {
    safeCall {
      endPointRef.askWithStream[CreateSecondaryFileResponse](
        CreateSecondaryFileRequest(regionId, maybeLocalId, totalLength, crc32),
        Unpooled.wrappedBuffer(content))
    }
  }

  def markSecondaryFileWritten(
                                regionId: Long,
                                localId: Long): Future[MarkSecondaryFileWrittenResponse] = {
    safeCall {
      endPointRef.askWithStream[MarkSecondaryFileWrittenResponse](
        MarkSecondaryFileWrittenRequest(regionId, localId))
    }
  }

  def deleteFile(fileId: FileId): Future[DeleteFileResponse] = {
    safeCall {
      endPointRef.ask[DeleteFileResponse](DeleteFileRequest(fileId))
    }
  }

  def processFiles[T](process: (Iterable[FileEntry]) => T): Future[T] = {
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

class WrongFileIdException(fileId: FileId) extends
  RegionFsClientException(s"wrong fileid: $fileId") {

}

class WrongFileStreamException(fileId: FileId) extends
  RegionFsClientException(s"wrong stream of file: $fileId") {

}

class ServerRaisedException(cause: Throwable) extends
  RegionFsClientException(s"got a server side error: ${cause.getMessage}") {

}