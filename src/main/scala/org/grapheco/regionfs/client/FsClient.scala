package org.grapheco.regionfs.client

import java.io.InputStream
import java.nio.ByteBuffer

import io.netty.buffer.Unpooled
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.{HippoEndpointRef, HippoRpcEnv, HippoRpcEnvFactory}
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import org.grapheco.commons.util.Logging
import org.grapheco.regionfs._
import org.grapheco.regionfs.server.RegionStatus
import org.grapheco.regionfs.util._

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

/**
  * a client to regionfs servers
  */
class FsClient(zks: String) extends Logging {
  val zookeeper = ZooKeeperClient.create(zks)
  val globalSetting = zookeeper.loadGlobalSetting()
  val clientFactory = new FsNodeClientFactory(globalSetting);
  val ring = new Ring[Int]()

  val consistencyStrategy: ConsistencyStrategy = ConsistencyStrategy.create(
    globalSetting.consistencyStrategy,
    clientOf(_), ring.!(_),
    (regionId: Long, nodeWithRevisions: Array[(Int, Long)]) => {
      arrayRegionWithNode ++= nodeWithRevisions.map(regionId -> _._1)
      mapRegionWithNodes.getOrElseUpdate(regionId, mutable.Map()) ++= nodeWithRevisions
    });

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

      def onUpdated(t: (Int, RpcAddress)): Unit = {
      }

      def onInitialized(batch: Iterable[(Int, RpcAddress)]): Unit = {
        this.synchronized {
          mapNodeWithAddress ++= batch
          ring ++= batch.map(_._1)
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
  val arrayRegionWithNode = mutable.Set[(Long, Int)]()
  val mapRegionWithNodes = mutable.Map[Long, mutable.Map[Int, Long]]()

  val regionsWatcher = zookeeper.watchRegionList(
    new ParsedChildNodeEventHandler[(Long, Int, Long)] {
      override def onCreated(t: (Long, Int, Long)): Unit = {
        arrayRegionWithNode += t._1 -> t._2
        mapRegionWithNodes.getOrElseUpdate(t._1, mutable.Map[Int, Long]()) += (t._2 -> t._3)
      }

      override def onUpdated(t: (Long, Int, Long)): Unit = {
        mapRegionWithNodes.synchronized {
          mapRegionWithNodes(t._1).update(t._2, t._3)
        }
      }

      override def onInitialized(batch: Iterable[(Long, Int, Long)]): Unit = {
        this.synchronized {
          arrayRegionWithNode ++= batch.map(x => x._1 -> x._2)
          mapRegionWithNodes ++= batch.groupBy(_._1).map(x =>
            x._1 -> (mutable.Map[Int, Long]() ++ x._2.map(x => x._2 -> x._3)))
        }
      }

      override def onDeleted(t: (Long, Int, Long)): Unit = {
        arrayRegionWithNode -= t._1 -> t._2
        mapRegionWithNodes -= t._1
      }

      override def accepts(t: (Long, Int, Long)): Boolean = {
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
    assertNodesNotEmpty();
    val crc32 =
      if (globalSetting.enableCrc) {
        CrcUtils.computeCrc32(content.duplicate())
      }
      else {
        0
      }

    consistencyStrategy.writeFile(writerSelector.select(), crc32, content);
  }

  def readFile[T](fileId: FileId, rpcTimeout: Duration): InputStream = {
    assertNodesNotEmpty()

    val regionOwnerNodes = mapRegionWithNodes.get(fileId.regionId)
    if (regionOwnerNodes.isEmpty)
      throw new WrongFileIdException(fileId);

    consistencyStrategy.readFile(fileId, regionOwnerNodes.get.toMap, rpcTimeout)
  }

  def deleteFile[T](fileId: FileId): Future[Boolean] = {
    consistencyStrategy.deleteFile(fileId, mapRegionWithNodes(fileId.regionId).toMap)
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
class FsNodeClient(globalSetting: GlobalSetting, val endPointRef: HippoEndpointRef, val remoteAddress: RpcAddress)
  extends Logging {
  implicit val executionContext: ExecutionContext = endPointRef.rpcEnv.executionContext

  private def safeCall[T](body: => T): T = {
    try {
      body
    }
    catch {
      case e: Throwable => throw new ServerRaisedException(e)
    }
  }

  def createFile(crc32: Long, content: ByteBuffer): Future[(FileId, Array[(Int, Long)])] = {
    safeCall {
      endPointRef.askWithStream[CreateFileResponse](
        CreateFileRequest(content.remaining(), crc32),
        Unpooled.wrappedBuffer(content)
      ).map(x => (x.fileId, x.nodes))
    }
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

  def deleteFile(fileId: FileId): Future[Boolean] = {
    safeCall {
      endPointRef.ask[DeleteFileResponse](
        DeleteFileRequest(fileId)).map(_.success)
    }
  }

  def readFile[T](fileId: FileId, rpcTimeout: Duration): InputStream = {
    safeCall {
      endPointRef.getInputStream(
        ReadFileRequest(fileId),
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