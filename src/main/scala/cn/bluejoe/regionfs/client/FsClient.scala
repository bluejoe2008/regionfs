package cn.bluejoe.regionfs.client

import java.io.InputStream

import cn.bluejoe.regionfs._
import cn.bluejoe.util.Logging
import io.netty.buffer.ByteBuf
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.{HippoRpcEnv, HippoRpcEnvFactory}
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration

/**
  * a client to regionfs servers
  */
class FsClient(zks: String) extends Logging {
  val zookeeper = ZooKeeperClient.create(zks)

  //get all nodes
  val nodes = new UpdatingNodeList(zookeeper, { _ => true }).start()
  //get all regions
  val regions = new UpdatingRegionList(zookeeper, { _ => true }).start()

  val selector = new RoundRobinSelector(nodes.mapNodeClients);

  private def assertNodesNotEmpty() {
    if (nodes.mapNodeClients.isEmpty) {
      throw new RegionFsClientException("no serving data nodes")
    }
  }

  def writeFile(is: InputStream, totalLength: Long): Future[FileId] = {
    assertNodesNotEmpty();

    val client = selector.select()
    client.writeFile(is, totalLength)
  }

  def readFile[T](fileId: FileId, rpcTimeout: Duration): InputStream = {
    assertNodesNotEmpty();
    val nodeId = (fileId.regionId >> 16).toInt;
    //regions.mapRegionNodes(fileId.regionId).apply(0)
    val nodeClient = nodes.mapNodeClients(nodeId)
    nodeClient.readFile(fileId, rpcTimeout)
  }

  def close = {
    nodes.stop()
    regions.stop()
    zookeeper.close()
  }
}

/**
  * FsNodeClient factory
  */
object FsNodeClient {
  val rpcEnv: HippoRpcEnv = {
    val rpcConf = new RpcConf()
    val config = RpcEnvClientConfig(rpcConf, "regionfs-client")
    HippoRpcEnvFactory.create(config)
  }

  def connect(remoteAddress: RpcAddress): FsNodeClient = {
    new FsNodeClient(rpcEnv, remoteAddress)
  }

  def connect(remoteAddress: String): FsNodeClient = {
    val pair = remoteAddress.split(":")
    new FsNodeClient(rpcEnv, RpcAddress(pair(0), pair(1).toInt))
  }

  override def finalize(): Unit = {
    rpcEnv.shutdown()
  }
}

/**
  * an FsNodeClient is an underline client used by FsClient
  * it sends raw messages (e.g. SendCompleteFileRequest) to NodeServer and handles responses
  */
class FsNodeClient(rpcEnv: HippoRpcEnv, val remoteAddress: RpcAddress) extends Logging {
  val endPointRef = rpcEnv.setupEndpointRef(RpcAddress(remoteAddress.host, remoteAddress.port), "regionfs-service");

  def close(): Unit = {
    rpcEnv.stop(endPointRef)
  }

  def writeFile(is: InputStream, totalLength: Long): Future[FileId] = {
    endPointRef.askWithStream[SendFileResponse](
      SendFileRequest(None, totalLength),
      (buf: ByteBuf) => {
        buf.writeBytes(is, totalLength.toInt)
      }).map(_.fileId)
  }

  def writeFileReplica(is: InputStream, totalLength: Long, regionId: Long): Future[FileId] = {
    endPointRef.askWithStream[SendFileResponse](
      SendFileRequest(None, totalLength),
      (buf: ByteBuf) => {
        buf.writeBytes(is, totalLength.toInt)
      }).map(_.fileId)
  }

  def readFile[T](fileId: FileId, rpcTimeout: Duration): InputStream = {
    endPointRef.getInputStream(
      ReadFileRequest(fileId.regionId, fileId.localId),
      rpcTimeout)
  }
}

trait FsNodeSelector {
  def select(): FsNodeClient;
}

class RoundRobinSelector(nodes: mutable.Map[Int, FsNodeClient]) extends FsNodeSelector {
  var iter = nodes.iterator;

  def select(): FsNodeClient = {
    nodes.synchronized {
      if (!iter.hasNext)
        iter = nodes.iterator;

      iter.next()._2
    }
  }
}

class RegionFsException(msg: String, cause: Throwable = null)
  extends RuntimeException(msg, cause) {

}

class RegionFsClientException(msg: String, cause: Throwable = null)
  extends RegionFsException(msg, cause) {

}

class WriteFileException(msg: String, cause: Throwable = null)
  extends RegionFsClientException(msg: String, cause) {

}