package cn.graiph.blobfs

import java.io.InputStream

import cn.graiph.blobfs.util.Logging
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, RpcEnvClientConfig}
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Random
import scala.util.parsing.json.JSON

class BlobFsClientException(msg: String, cause: Throwable = null)
  extends RuntimeException(msg, cause) {

}

class FsClient(zks: String) extends Logging {
  val zk = new ZooKeeper(zks, 2000, new Watcher {
    override def process(event: WatchedEvent): Unit = {
      logger.debug("event:" + event);
    }
  });

  //get all nodes
  val clients = ArrayBuffer[FsNodeClient]();
  clients ++= zk.getChildren("/blobfs/nodes", new Watcher() {
    override def process(event: WatchedEvent): Unit = {
      logger.debug("event:" + event);
    }
  }).map(name => {
    val data = zk.getData("/blobfs/nodes/" + name, new Watcher {
      override def process(event: WatchedEvent): Unit = {
        //new nodes add? deletion?
      }
    }, null);

    val json = JSON.parseFull(new String(data)).get.asInstanceOf[Map[String, _]];
    FsNodeClient.connect(json("rpcAddress").asInstanceOf[String])
  });

  if (clients.isEmpty) {
    throw new BlobFsClientException("no serving data nodes");
  }

  def writeFile(is: InputStream, totalLength: Long): FileId = {
    val n = new Random().nextInt(clients.size);
    clients(n).writeFile(is, totalLength);
  }
}

object FsNodeClient {
  def connect(address: String): FsNodeClient = {
    val pair = address.split(":");
    new FsNodeClient(pair(0), pair(1).toInt);
  }
}

class FsNodeClient(host: String, port: Int) {
  val rpcConf = new RpcConf()
  val config = RpcEnvClientConfig(rpcConf, "blobfs-client")
  val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)
  val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress(host, port), "blobfs-service")

  val CHUNK_SIZE: Int = 10240

  def writeFile(is: InputStream, totalLength: Long): FileId = {
    //small file
    if (totalLength <= CHUNK_SIZE) {
      val bytes = new Array[Byte](CHUNK_SIZE);
      val n = is.read(bytes);
      val res = Await.result(endPointRef.ask[SendCompleteFileResponse](SendCompleteFileRequest(bytes, totalLength)),
        Duration.apply("30s"));

      res.fileId;
    }
    else {
      //split files
      val res = Await.result(endPointRef.ask[StartSendChunksResponse](StartSendChunksRequest(totalLength)),
        Duration.apply("30s"));
      val transId = res.transId;

      var blocks = 0;
      var offset = 0;
      var n = 0;
      val futures = ArrayBuffer[Future[SendChunkResponse]]();
      try {
        while (n >= 0) {
          //10k
          val bytes = new Array[Byte](CHUNK_SIZE);
          n = is.read(bytes);
          if (n > 0) {
            //send this block
            val future: Future[SendChunkResponse] = endPointRef.ask[SendChunkResponse](
              SendChunkRequest(transId, bytes.slice(0, n), offset, n, blocks))

            futures += future;
            future.onComplete {
              case scala.util.Success(value) => {
                println(s"Got the result = $value")
              }

              case scala.util.Failure(e) => println(s"Got error: $e")
            }

            blocks += 1;
            offset += n;
          }
        }

        //yes, end of file
        //endPointRef.send(SendFileCompelte(fileId, blocks, length));
        //Await.result(CompletableFuture.allOf(futures), Duration.apply("30s"))
        //TODO: ugly code, use CompletableFuture instead, if possible:)
        val res = futures.map(future =>
          Await.result(future, Duration.apply("30s")))

        res.head.fileId;
      }
      catch {
        case e: Throwable =>
          e.printStackTrace();

          if (blocks > 0) {
            endPointRef.send(DiscardChunksRequest(transId));
          }

          throw e;
      }
    }
  }
}