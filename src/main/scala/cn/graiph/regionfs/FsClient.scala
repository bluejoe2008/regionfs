package cn.graiph.regionfs

import java.io.{ByteArrayInputStream, InputStream}

import cn.graiph.regionfs.util.{IteratorUtils, Logging, StreamUtils}
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnv, RpcEnvClientConfig}
import org.apache.commons.io.IOUtils
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag

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
    client.writeFileAsync(is, totalLength)
  }

  def readFile[T](fileId: FileId): InputStream = {
    //FIXME: if the region is created just now, the delay of zkwatch will cause failure of regionNodes.map(fileId.regionId)
    val nodeAddress = regionNodes.map(fileId.regionId)
    val client = nodes.map(nodeAddress)
    client.readFile(fileId)
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
  private val endPointRef = createEndPointRef

  private def createEndPointRef() = {
    val rpcConf = new RpcConf()
    val config = RpcEnvClientConfig(rpcConf, "regionfs-client")
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)

    rpcEnv.setupEndpointRef(RpcAddress(remoteAddress.host, remoteAddress.port), "regionfs-service")
  }

  def close(): Unit = {
    rpcEnv.stop(endPointRef)
  }

  def writeFileAsync(is: InputStream, totalLength: Long): Future[FileId] = {
    //small file
    if (totalLength <= Constants.WRITE_CHUNK_SIZE) {
      val bytes = IOUtils.toByteArray(is)
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
          val bytes = new Array[Byte](Constants.WRITE_CHUNK_SIZE)
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

  def ask[T: ClassTag](request: AnyRef, timeOut: Duration = Duration.Inf): T = {
    Await.result(endPointRef.ask[T](request), timeOut)
  }

  def askAsync[T: ClassTag](request: AnyRef): Future[T] = {
    endPointRef.ask[T](request)
  }

  def askStream[T <: StreamingResult](request: AnyRef, pageSize: Int): Iterator[T] = {
    var txId: Long = -1;
    var hasMore = true
    IteratorUtils.concatIterators { (index: Int) =>
      if (!hasMore) {
        None
      }
      else {
        val res = {
          if (index == 0) {
            ask[StreamResponse](StartStreamRequest(request, pageSize))
          }
          else {
            ask[StreamResponse](GetNextPageRequest(txId))
          }
        }

        if (txId < 0)
          txId = res.txId;

        hasMore = res.hasMore
        Some(res.page.iterator.map(_.asInstanceOf[T]))
      }
    }
  }

  def readFile2[T](fileId: FileId): InputStream = {
    StreamUtils.
      of(askStream[ReadFileResponseDetail](ReadFileRequest(fileId.regionId, fileId.localId), 1).
        flatMap(_.content.iterator))
  }

  def readFile1[T](fileId: FileId): InputStream = {
    var offset: Long = 0;
    StreamUtils.concatStreams {
      if (offset == -1) {
        None
      }
      else {
        val res = Await.result(endPointRef.ask[ReadChunkResponse](
          ReadChunkRequest(fileId.regionId, fileId.localId, offset, Constants.READ_CHUNK_SIZE)), Duration.Inf)

        offset = res.nextOffset
        Some(new ByteArrayInputStream(res.content))
      }
    }
  }

  def readFile(fileId: FileId): InputStream = {
    new ByteArrayInputStream(Await.result(endPointRef.ask[ReadCompleteFileResponse](
      ReadCompleteFileRequest(fileId.regionId, fileId.localId)), Duration.Inf).content)
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