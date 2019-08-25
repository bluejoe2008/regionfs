package cn.graiph.blobfs

import java.io.InputStream
import java.util.UUID

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

class BlobFsClientException(msg: String, cause: Throwable = null) extends RuntimeException(msg, cause) {

}

case class NodeStat(nodeId: Int, rpcAddress: String) {

}

class FsClient(zks: String) extends Logging {
  val zk = new ZooKeeper(zks, 2000, new Watcher {
    override def process(event: WatchedEvent): Unit = {
      logger.debug("event:" + event);
    }
  });

  //get all nodes
  val nodes = ArrayBuffer[NodeStat]();
  nodes ++= zk.getChildren("/blobfs/nodes", new Watcher() {
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
    NodeStat(json("nodeId").asInstanceOf[Double].toInt, json("rpcAddress").asInstanceOf[String]);
  });

  if (nodes.isEmpty) {
    throw new BlobFsClientException("no serving data nodes");
  }

  def writeFile(is: InputStream, totalLength: Long): FileId = {
    val n = new Random().nextInt(nodes.size);
    FsNodeClient.connect(nodes(n).rpcAddress).writeFile(is, totalLength);
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
  val config = RpcEnvClientConfig(rpcConf, "hello-client")
  val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)
  val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress(host, port), "blobfs-service")

  def writeFile(is: InputStream, totalLength: Long): FileId = {
    //split files, if required
    var blocks = 0;
    var n = 0;
    var length = 0;
    val uuid = UUID.randomUUID().toString;
    val futures = ArrayBuffer[Future[(Int, Option[FileId])]]();
    try {
      while (n >= 0) {
        val bytes = new Array[Byte](10240);
        n = is.read(bytes);
        if (n > 0) {
          //send this block

          val future: Future[(Int, Option[FileId])] = endPointRef.ask[(Int, Option[FileId])](
            SendFileBlock(uuid, bytes.slice(0, n), length, n, totalLength, blocks))

          futures += future;

          future.onComplete {
            case scala.util.Success(value) => {
              println(s"Got the result = $value")
            }

            case scala.util.Failure(e) => println(s"Got error: $e")
          }

          blocks += 1;
          length += n;
        }
      }

      //yes, end of file
      //endPointRef.send(SendFileCompelte(fileId, blocks, length));
      //Await.result(CompletableFuture.allOf(futures), Duration.apply("30s"))
      //TODO: ugly code, use CompletableFuture instead, if possible:)
      futures.foreach(future =>
        Await.result(future, Duration.apply("30s")))

      val fileId = futures.flatMap(_.value).map(_.get._2).filter(_.isDefined).map(_.get).head;
      fileId;
    }
    catch {
      case e: Throwable =>
        e.printStackTrace();

        if (blocks > 0) {
          endPointRef.send(SendFileDiscard(uuid));
        }

        throw e;
    }
  }
}