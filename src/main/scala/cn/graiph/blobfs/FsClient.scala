package cn.graiph.blobfs

import java.io.InputStream
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong

import cn.graiph.blobfs.util.Logging
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnv, RpcEnvClientConfig}
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Random

class BlobFsClientException(msg: String, cause: Throwable = null)
  extends RuntimeException(msg, cause) {

}

class WriteFileException(msg: String, cause: Throwable = null) extends BlobFsClientException(msg: String, cause) {

}

class FsClient(zks: String) extends Logging {
  val zk = new ZooKeeper(zks, 2000, new Watcher {
    override def process(event: WatchedEvent): Unit = {
      logger.debug("event:" + event)
    }
  })

  //get all nodes
  val clients = ArrayBuffer[FsNodeClient]()
  //watching!
  clients ++= zk.getChildren("/blobfs/nodes", new Watcher() {
    override def process(event: WatchedEvent): Unit = {
      println("event:" + event)
    }
  }).map { name =>
    FsNodeClient.connect(NodeAddress.fromString(name, "_"))
  }

  //TODO: replica vs. node number?
  if (clients.isEmpty) {
    throw new BlobFsClientException("no serving data nodes")
  }

  //get stats
  val MAX_REGION_SZIE = 1024 * 1024 * 128

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
    val client = clients(rand.nextInt(clients.length))
    //logger.debug(s"chose client: $client")
    client.writeFileAsync(is, totalLength)
  }
}

object FsNodeClient {
  def connect(remoteAddress: NodeAddress): FsNodeClient = {
    new FsNodeClient(remoteAddress)
  }

  def connect(remoteAddress: String): FsNodeClient = {
    val pair = remoteAddress.split(":")
    new FsNodeClient(NodeAddress(pair(0), pair(1).toInt))
  }
}

object NodeAddress {
  def fromString(url: String, delimeter: String = ":") = {
    val pair = url.split(delimeter)
    NodeAddress(pair(0), pair(1).toInt)
  }
}

case class NodeAddress(host: String, port: Int) {

}

case class FsNodeClient(val remoteAddress: NodeAddress) extends Logging {
  val endPointRef = {
    val rpcConf = new RpcConf()
    val config = RpcEnvClientConfig(rpcConf, "blobfs-client")
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)

    rpcEnv.setupEndpointRef(RpcAddress(remoteAddress.host, remoteAddress.port), "blobfs-service")
  }

  //10K each chunk
  val CHUNK_SIZE: Int = 10240

  //region is not assigned
  def writeFileAsync(is: InputStream, totalLength: Long): Future[FileId] = {
    //choose a region
    //small file
    if (totalLength <= CHUNK_SIZE) {
      val bytes = new Array[Byte](CHUNK_SIZE)
      endPointRef.ask[SendCompleteFileResponse](
        SendCompleteFileRequest(None, bytes, totalLength)).
        map(_.fileId)
    }
    else {
      //split files
      val res = Await.result(endPointRef.ask[StartSendChunksResponse](
        StartSendChunksRequest(None, totalLength)),
        Duration.apply("30s"))

      val transId = res.transId
      var chunks = 0
      var offset = 0
      var n = 0

      val waitLatch = new CountDownLatch(1)
      val results = new ArrayBuffer[FileId]()
      val waitByteCount = new AtomicLong(0)

      try {
        while (n >= 0) {
          //10k
          val bytes = new Array[Byte](CHUNK_SIZE)
          n = is.read(bytes)

          if (n > 0) {
            //send this chunk
            val future: Future[SendChunkResponse] = endPointRef.ask[SendChunkResponse](
              SendChunkRequest(transId, bytes.slice(0, n), offset, n, chunks))

            future.onComplete {
              case scala.util.Success(value) => {
                if (value.fileId.isDefined) {
                  results.synchronized {
                    results += value.fileId.get
                  }
                }

                val bc = waitByteCount.addAndGet(value.chunkLength)
                if (bc >= totalLength) {
                  waitLatch.countDown()
                }
              }

              case scala.util.Failure(e) => {
                logger.warn(s"failure: $e")
              }
            }

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
        waitLatch.await()
        results.head
      }
    }
  }
}