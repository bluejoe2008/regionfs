package cn.bluejoe.regionfs.client

import java.io.InputStream

import cn.bluejoe.util.Logging
import cn.bluejoe.regionfs._
import io.netty.buffer.ByteBuf
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.{HippoRpcEnv, HippoRpcEnvFactory}
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class RegionFsClientException(msg: String, cause: Throwable = null)
  extends RuntimeException(msg, cause) {

}

class WriteFileException(msg: String, cause: Throwable = null)
  extends RegionFsClientException(msg: String, cause) {

}

/**
  * a client to regionfs servers
  */
class FsClient(zks: String) extends Logging {
  val zk = new ZooKeeper(zks, 2000, new Watcher {
    override def process(event: WatchedEvent): Unit = {
      if (logger.isDebugEnabled)
        logger.debug("event:" + event)
    }
  })

  //get all nodes
  val nodes = new NodeWatcher(zk, { _ => true })
  val regionNodes = new RegionNodesWatcher(zk)
  val selector = new RoundRobinSelector(nodes.clients.toList);

  //TODO: replica vs. node number?
  if (nodes.isEmpty) {
    throw new RegionFsClientException("no serving data nodes")
  }

  def writeFiles(inputs: Iterable[(InputStream, Long)]): Iterable[FileId] = {
    inputs.map(x =>
      writeFileAsync(x._1, x._2)).
      map(Await.result(_, Duration.Inf))
  }

  def writeFile(is: InputStream, totalLength: Long): FileId = {
    Await.result(writeFileAsync(is: InputStream, totalLength: Long), Duration.Inf)
  }

  def writeFileAsync(is: InputStream, totalLength: Long): Future[FileId] = {
    //choose a client
    val client = selector.select()
    //logger.debug(s"chose client: $client")
    client.writeFile(is, totalLength)
  }

  def readFile[T](fileId: FileId): InputStream = {
    //FIXME: if the region is created just now, the delay of zkwatch will cause failure of regionNodes.map(fileId.regionId)
    val nodeAddress = regionNodes.map(fileId.regionId)
    val nodeClient = nodes.map(nodeAddress)
    nodeClient.readFile(fileId)
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

  def connect(remoteAddress: NodeAddress): FsNodeClient = {
    new FsNodeClient(rpcEnv, remoteAddress)
  }

  def connect(remoteAddress: String): FsNodeClient = {
    val pair = remoteAddress.split(":")
    new FsNodeClient(rpcEnv, NodeAddress(pair(0), pair(1).toInt))
  }

  override def finalize(): Unit = {
    rpcEnv.shutdown()
  }
}

object NodeAddress {
  def fromString(url: String, separtor: String = ":") = {
    val pair = url.split(separtor)
    NodeAddress(pair(0), pair(1).toInt)
  }
}

/**
  * address of a node
  */
case class NodeAddress(host: String, port: Int) {

}

/**
  * an FsNodeClient is an underline client used by FsClient
  * it sends raw messages (e.g. SendCompleteFileRequest) to NodeServer and handles responses
  */
case class FsNodeClient(rpcEnv: HippoRpcEnv, val remoteAddress: NodeAddress) extends Logging {

  val endPointRef = rpcEnv.setupEndpointRef(RpcAddress(remoteAddress.host, remoteAddress.port), "regionfs-service");

  def close(): Unit = {
    rpcEnv.stop(endPointRef)
  }

  def writeFile(is: InputStream, totalLength: Long): Future[FileId] = {
    endPointRef.askWithStream[SendFileResponse](SendFileRequest(None, totalLength), (buf: ByteBuf) => {
      buf.writeBytes(is, totalLength.toInt)
    }).map(_.fileId)
  }

  def writeFileReplica(is: InputStream, totalLength: Long, regionId: Long): Future[FileId] = {
    endPointRef.askWithStream[SendFileResponse](SendFileRequest(None, totalLength), (buf: ByteBuf) => {
      buf.writeBytes(is, totalLength.toInt)
    }).map(_.fileId)
  }

  def readFile[T](fileId: FileId): InputStream = {
    endPointRef.getInputStream(ReadFileRequest(fileId.regionId, fileId.localId))
  }
}

trait FsNodeSelector {
  def select(): FsNodeClient;
}

class RoundRobinSelector(nodes: List[FsNodeClient]) extends FsNodeSelector {
  var index = -1;

  def select(): FsNodeClient = {
    this.synchronized {
      index += 1
      if (index >= nodes.length) {
        index = 0;
      }
      nodes.apply(index)
    }
  }
}