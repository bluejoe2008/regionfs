package cn.graiph.regionfs

import java.io.InputStream

import cn.graiph.regionfs.util.Logging
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnv, RpcEnvClientConfig}
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.reflect.io.Streamable.Bytes
import scala.util.Random

class RegionFsClientException(msg: String, cause: Throwable = null)
  extends RuntimeException(msg, cause) {

}

class WriteFileException(msg: String, cause: Throwable = null) extends RegionFsClientException(msg: String, cause) {

}

/**
  * a client to regionfs servers
  */
class FsClient(zks: String) extends Logging {
  val zk = new ZooKeeper(zks, 2000, new Watcher {
    override def process(event: WatchedEvent): Unit = {
      logger.debug("event:" + event)
    }
  })

  //get all nodes
  val nodes = new WatchingNodes(zk, { _ => true })

  //TODO: replica vs. node number?
  if (nodes.isEmpty) {
    throw new RegionFsClientException("no serving data nodes")
  }

  //max region size: 128MB
  //TODO: configurable?
  val MAX_REGION_SIZE = 1024 * 1024 * 128

  def writeFiles(inputs: Iterable[(InputStream, Long)]): Iterable[FileId] = {
    inputs.map(x =>
      writeFileAsync(x._1, x._2)).
      map(Await.result(_, Duration.Inf))
  }

  def writeFile(is: InputStream, totalLength: Long): FileId = {
    Await.result(writeFileAsync(is: InputStream, totalLength: Long), Duration.Inf)
  }

  val rand = new Random();

  def writeFileAsync(is: InputStream, totalLength: Long): Future[FileId] = {
    //choose a client
    val clients = nodes.clients.toArray;
    val client = clients(rand.nextInt(clients.size))
    //logger.debug(s"chose client: $client")
    client.writeFileAsync(is, totalLength)
  }

  def readFile(fileId: FileId): Array[Byte] = {
    Await.result(readFileAsync(fileId: FileId), Duration.Inf)
  }

  def readFileAsync(fileId: FileId): Future[Array[Byte]] = {
    val watchRegionNodes = new WatchingRegionNodes(zk)
    val nodeAddress = watchRegionNodes.map(fileId.regionId)
    val client = nodes.map(nodeAddress)
    client.readFileAsync(fileId)
  }

}

/**
  * FsNodeClient factory
  */
object FsNodeClient {
  val rpcEnv: RpcEnv = {
    val rpcConf = new RpcConf()
    val config = RpcEnvClientConfig(rpcConf, "regionfs-client")
    NettyRpcEnvFactory.create(config)
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
case class FsNodeClient(rpcEnv: RpcEnv, val remoteAddress: NodeAddress) extends Logging {
  val endPointRef = {
    val rpcConf = new RpcConf()
    val config = RpcEnvClientConfig(rpcConf, "regionfs-client")
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)

    rpcEnv.setupEndpointRef(RpcAddress(remoteAddress.host, remoteAddress.port), "regionfs-service")
  }

  def close(): Unit = {
    rpcEnv.stop(endPointRef)
  }

  //10K each chunk
  //TODO: use ConfigServer
  val CHUNK_SIZE: Int = 10240

  def writeFileAsync(is: InputStream, totalLength: Long): Future[FileId] = {
    //small file
    if (totalLength <= CHUNK_SIZE) {
      val bytes = new Array[Byte](CHUNK_SIZE)
      endPointRef.ask[SendCompleteFileResponse](
        SendCompleteFileRequest(None, bytes, totalLength)).
        map(_.fileId)
    }
    else {
      //split large files into chunks
      val res = Await.result(endPointRef.ask[StartSendChunksResponse](
        StartSendChunksRequest(None, totalLength)),
        Duration.Inf)

      val transId = res.transId
      //TODO: too many vars
      var chunks = 0
      var offset = 0
      var n = 0

      val futures = ArrayBuffer[Future[SendChunkResponse]]()

      try {
        while (n >= 0) {
          //10k
          val bytes = new Array[Byte](CHUNK_SIZE)
          n = is.read(bytes)

          if (n > 0) {
            //send this chunk
            val future: Future[SendChunkResponse] = endPointRef.ask[SendChunkResponse](
              SendChunkRequest(transId, bytes.slice(0, n), offset, n, chunks))

            futures += future
            chunks += 1
            offset += n
          }
        }
      }
      catch {
        case e: Throwable =>
          e.printStackTrace()

          if (chunks > 0) {
            //endPointRef.send(DiscardChunksRequest(transId))
          }

          throw e
      }

      Future {
        //awaits all chunks are received
        //one response should contain a Some(FieldId), while others returns None
        futures.map(Await.result(_, Duration.Inf)).find(_.fileId.isDefined).get.fileId.get
      }
    }
  }

  def readFileAsync(fileId: FileId): Future[Array[Byte]] = {
    endPointRef.ask[ReadCompleteFileResponse](
      ReadCompleteFileRequest(fileId.regionId,fileId.localId)).map(_.content)
  }
}