package org.grapheco.regionfs.client

import java.io.InputStream
import java.nio.ByteBuffer

import io.netty.buffer.ByteBuf
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.{HippoEndpointRef, HippoRpcEnv, HippoRpcEnvFactory}
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import org.grapheco.commons.util.Logging
import org.grapheco.regionfs._
import org.grapheco.regionfs.server.RegionStatus
import org.grapheco.regionfs.util._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * a client to regionfs servers
  */
class FsClient(zks: String) extends Logging {
  val zookeeper = ZooKeeperClient.create(zks)
  val globalSetting = zookeeper.loadGlobalSetting()
  val clientFactory = new FsNodeClientFactory(globalSetting);
  val ring = new Ring[Int]()

  val consistencyStrategy: ConsistencyStrategy = ConsistencyStrategy.create(
    globalSetting.consistencyStrategy, clientOf(_), ring.!(_));

  //get all nodes
  val cachedClients = mutable.Map[Int, FsNodeClient]()
  val mapNodeWithAddress = mutable.Map[Int, RpcAddress]()
  val nodesWatcher = zookeeper.watchNodeList(
    new ParsedChildNodeEventHandler[(Int, RpcAddress)] {
      override def onCreated(t: (Int, RpcAddress)): Unit = {
        mapNodeWithAddress.synchronized {
          mapNodeWithAddress += t
        }
        ring.synchronized {
          ring += t._1
        }
      }

      override def onDeleted(t: (Int, RpcAddress)): Unit = {
        mapNodeWithAddress.synchronized {
          mapNodeWithAddress -= t._1
        }
        ring.synchronized {
          ring -= t._1
        }
      }

      override def accepts(t: (Int, RpcAddress)): Boolean = {
        true
      }
    })

  //get all regions
  //32768->(1,2), 32769->(1), ...
  val arrayRegionWithNode = ArrayBuffer[(Long, Int)]()
  val mapRegionWithNodes = mutable.Map[Long, ArrayBuffer[Int]]()

  val regionsWatcher = zookeeper.watchRegionList(
    new ParsedChildNodeEventHandler[(Long, Int)] {
      override def onCreated(t: (Long, Int)): Unit = {
        arrayRegionWithNode += t
        mapRegionWithNodes.getOrElseUpdate(t._1, ArrayBuffer()) += t._2
      }

      override def onDeleted(t: (Long, Int)): Unit = {
        arrayRegionWithNode -= t
        mapRegionWithNodes -= t._1
      }

      override def accepts(t: (Long, Int)): Boolean = {
        true
      }
    })

  val writerSelector = new RoundRobinSelector(ring);

  def clientOf(nodeId: Int): FsNodeClient = {
    cachedClients.getOrElseUpdate(nodeId,
      clientFactory.of(mapNodeWithAddress(nodeId)))
  }

  private def assertNodesNotEmpty() {
    if (mapNodeWithAddress.isEmpty) {
      if (logger.isTraceEnabled) {
        logger.trace(zookeeper.readNodeList().mkString(","))
      }

      throw new RegionFsClientException("no serving data nodes")
    }
  }

  def writeFile(content: ByteBuffer): Future[FileId] = {
    val client = getWriterClient
    //prepare
    val (regionId: Long, fileId: FileId, regionOwnerNodes: Array[Int]) =
      Await.result(client.prepareToWriteFile(content), Duration("1s"))

    val crc32 =
      if (globalSetting.enableCrc) {
        CrcUtils.computeCrc32(content.duplicate())
      }
      else {
        0
      }

    consistencyStrategy.writeFile(fileId, regionOwnerNodes, crc32, content.duplicate());
  }

  def getWriterClient: FsNodeClient = {
    assertNodesNotEmpty();
    clientOf(writerSelector.select())
  }

  def readFile[T](fileId: FileId, rpcTimeout: Duration): InputStream = {
    assertNodesNotEmpty()

    val regionOwnerNodes = mapRegionWithNodes.get(fileId.regionId)
    if (regionOwnerNodes.isEmpty)
      throw new WrongFileIdException(fileId);

    consistencyStrategy.readFile(fileId, regionOwnerNodes.get.toArray, rpcTimeout)
  }

  def deleteFile[T](fileId: FileId): Future[Boolean] = {
    consistencyStrategy.deleteFile(fileId, mapRegionWithNodes(fileId.regionId).toArray)
  }

  def close = {
    nodesWatcher.close()
    regionsWatcher.close()
    clientFactory.close()
    zookeeper.close()
  }
}

/**
  * FsNodeClient factory
  */
class FsNodeClientFactory(globalSetting: GlobalSetting) {
  val refs = mutable.Map[RpcAddress, HippoEndpointRef]();

  val rpcEnv: HippoRpcEnv = {
    val rpcConf = new RpcConf()
    val config = RpcEnvClientConfig(rpcConf, "regionfs-client")
    HippoRpcEnvFactory.create(config)
  }

  def of(remoteAddress: RpcAddress): FsNodeClient = {
    val endPointRef = rpcEnv.synchronized {
      refs.getOrElseUpdate(remoteAddress,
        rpcEnv.setupEndpointRef(RpcAddress(remoteAddress.host, remoteAddress.port), "regionfs-service"));
    }

    new FsNodeClient(globalSetting: GlobalSetting, endPointRef, remoteAddress)
  }

  def of(remoteAddress: String): FsNodeClient = {
    val pair = remoteAddress.split(":")
    of(RpcAddress(pair(0), pair(1).toInt))
  }

  def close(): Unit = {
    refs.foreach(x => rpcEnv.stop(x._2))
    refs.clear()
    rpcEnv.shutdown()
  }
}

/**
  * an FsNodeClient is an underline client used by FsClient
  * it sends raw messages (e.g. SendCompleteFileRequest) to NodeServer and handles responses
  */
class FsNodeClient(globalSetting: GlobalSetting, val endPointRef: HippoEndpointRef, val remoteAddress: RpcAddress) extends Logging {
  private def safeCall[T](body: => T): T = {
    try {
      body
    }
    catch {
      case e: Throwable => throw new ServerRaisedException(e)
    }
  }

  def prepareToWriteFile(content: ByteBuffer): Future[(Long, FileId, Array[Int])] = {
    safeCall {
      endPointRef.ask[PrepareToWriteFileResponse](
        PrepareToWriteFileRequest(content.remaining())).map(x => (x.regionId, x.fileId, x.regionOwnerNodes))
    }
  }

  def writeFile(regionId: Long, crc32: Long, fileId: FileId, content: ByteBuffer): Future[FileId] = {
    safeCall {
      endPointRef.askWithStream[SendFileResponse](
        SendFileRequest(regionId, fileId, content.remaining(), crc32),
        (buf: ByteBuf) => {
          buf.writeBytes(content)
        }).map(_.fileId)
    }
  }

  def deleteFile(fileId: FileId): Future[Boolean] = {
    safeCall {
      endPointRef.ask[DeleteFileResponse](
        DeleteFileRequest(fileId.regionId, fileId.localId)).map(_.success)
    }
  }

  def readFile[T](fileId: FileId, rpcTimeout: Duration): InputStream = {
    safeCall {
      endPointRef.getInputStream(
        ReadFileRequest(fileId.regionId, fileId.localId),
        rpcTimeout)
    }
  }

  def getPatchInputStream(regionId: Long, revision: Long, rpcTimeout: Duration): InputStream = {
    safeCall {
      endPointRef.getInputStream(
        GetRegionPatchRequest(regionId, revision),
        rpcTimeout)
    }
  }

  def getRegionStatus(regionIds: Array[Long]): Future[Array[RegionStatus]] = {
    safeCall {
      endPointRef.ask[GetRegionStatusResponse](
        GetRegionStatusRequest(regionIds)).map(_.statusList)
    }
  }
}

trait FsNodeSelector {
  def select(): Int;
}

class RoundRobinSelector(nodes: Ring[Int]) extends FsNodeSelector {
  def select(): Int = {
    nodes !
  }
}

class RegionFsClientException(msg: String, cause: Throwable = null)
  extends RegionFsException(msg, cause) {

}

class WrongFileIdException(fileId: FileId) extends
  RegionFsClientException(s"wrong fileid: $fileId") {

}

class ServerRaisedException(cause: Throwable) extends
  RegionFsClientException(s"got a server side error: ${cause.getMessage}") {

}

trait ConsistencyStrategy {
  def deleteFile(fileId: FileId, regionOwnerNodes: Array[Int]): Future[Boolean]

  def readFile[T](fileId: FileId, regionOwnerNodes: Array[Int], rpcTimeout: Duration): InputStream

  def writeFile(fileIdExpected: FileId, regionOwnerNodes: Array[Int], crc32: Long, content: ByteBuffer): Future[FileId]
}

object ConsistencyStrategy {
  def create(strategyType: Int, clientOf: (Int) => FsNodeClient,
             chooseNextNode: ((Int) => Boolean) => Option[Int]): ConsistencyStrategy =
    strategyType match {
      case Constants.CONSISTENCY_STRATEGY_EVENTUAL =>
        new EventualConsistencyStrategy(clientOf, chooseNextNode)
      case Constants.CONSISTENCY_STRATEGY_STRONG =>
        new StrongConsistencyStrategy(clientOf, chooseNextNode)
    }
}

class EventualConsistencyStrategy(clientOf: (Int) => FsNodeClient, chooseNextNode: ((Int) => Boolean) => Option[Int]) extends ConsistencyStrategy {
  def writeFile(fileIdExpected: FileId, regionOwnerNodes: Array[Int], crc32: Long, content: ByteBuffer): Future[FileId] = {
    //only primary region's being written is necessary
    clientOf((fileIdExpected.regionId >> 16).toInt).writeFile(fileIdExpected.regionId, crc32, fileIdExpected, content.duplicate())
  }

  def readFile[T](fileId: FileId, regionOwnerNodes: Array[Int], rpcTimeout: Duration): InputStream = {
    clientOf((fileId.regionId >> 16).toInt).readFile(fileId, rpcTimeout)
  }

  def deleteFile(fileId: FileId, regionOwnerNodes: Array[Int]): Future[Boolean] = {
    clientOf((fileId.regionId >> 16).toInt).deleteFile(fileId)
  }
}

class StrongConsistencyStrategy(clientOf: (Int) => FsNodeClient, chooseNextNode: ((Int) => Boolean) => Option[Int]) extends ConsistencyStrategy {
  def writeFile(fileIdExpected: FileId, regionOwnerNodes: Array[Int], crc32: Long, content: ByteBuffer): Future[FileId] = {
    val futures =
      regionOwnerNodes.map(clientOf(_).writeFile(fileIdExpected.regionId, crc32, fileIdExpected, content.duplicate()))

    Future {
      //TODO: consistency check
      futures.map(x => Await.result(x, Duration.Inf)).head
    }
  }

  def readFile[T](fileId: FileId, regionOwnerNodes: Array[Int], rpcTimeout: Duration): InputStream = {
    val map = regionOwnerNodes.map((_, 1)).toMap
    val maybeNodeId = chooseNextNode(map.contains(_))
    if (maybeNodeId.isEmpty)
      throw new WrongFileIdException(fileId);

    clientOf(maybeNodeId.get).readFile(fileId, rpcTimeout)
  }

  def deleteFile(fileId: FileId, regionOwnerNodes: Array[Int]): Future[Boolean] = {
    val futures = regionOwnerNodes.map(clientOf(_).deleteFile(fileId))

    if (futures.isEmpty)
      throw new WrongFileIdException(fileId);

    Future {
      futures.map(Await.result(_, Duration.Inf)).head
    }
  }
}