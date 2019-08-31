package cn.graiph.blobfs

import java.io.InputStream
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CountDownLatch, TimeUnit}

import cn.graiph.blobfs.util.Logging
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnv, RpcEnvClientConfig}
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.parsing.json.JSON

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
  clients ++= zk.getChildren("/blobfs/nodes", new Watcher() {
    override def process(event: WatchedEvent): Unit = {
      logger.debug("event:" + event)
    }
  }).map(name => {
    val data = zk.getData("/blobfs/nodes/" + name, new Watcher {
      override def process(event: WatchedEvent): Unit = {
        //new nodes add? deletion?
      }
    }, null)

    val json = JSON.parseFull(new String(data)).get.asInstanceOf[Map[String, _]]
    FsNodeClient.connect(json("rpcAddress").asInstanceOf[String])
  })

  //TODO: replica vs. node number?
  if (clients.isEmpty) {
    throw new BlobFsClientException("no serving data nodes")
  }

  //get stats
  val MAX_REGION_SZIE = 1024 * 1024 * 128
  val mutableNodeStats = mutable.Map[FsNodeClient, NodeStat]()
  doStat()

  //TODO: move to agent node?
  def doStat() = {
    val nodeStats = clients.map(client => {
      val ns = client.getNodeStat()
      client -> NodeStat(ns.maxRegionId, ns.regionCount, ns.sizeMap.filter(_.totalSize < MAX_REGION_SZIE))
    })

    mutableNodeStats.synchronized {
      mutableNodeStats.clear()
      mutableNodeStats ++= nodeStats
    }
  }

  val thread = new Thread(new Runnable {
    override def run(): Unit = {
      while (true) {
        Thread.sleep(1000)
        doStat()
      }
    }
  })

  thread.start()

  def writeFile(is: InputStream, totalLength: Long): FileId = {
    //[client1,NodeStat1][client2,NodeStat2]
    val nodeStats = mutableNodeStats.synchronized(mutableNodeStats.toArray)
    //exists available regions
    val available = nodeStats.filter(!_._2.sizeMap.isEmpty)
    if (!available.isEmpty) {
      val region = available.
        flatMap(x => x._2.sizeMap.map(x._1 -> _)). //[client1,(region11,size11)][client1,(region12,size12)][client2,(region21,size21)]
        groupBy(_._2.regionId). //[region11,[[client1, (region11,size11)],[client2, (region11,size11)]]
        map(x =>
        x._1 -> (x._2.head._2.totalSize -> x._2) //[region11,(size11,[[...]])]
      ).toArray.sortBy(_._2._1).head

      writeFile(region._2._2.map(_._1), region._1, is, totalLength)
    }
    else {
      val regionIdNew = (nodeStats.map(_._2.maxRegionId) ++ Array(0)).max + 1
      val selected = nodeStats.sortBy(_._2.regionCount).take(2)
      selected.foreach(_._1.createRegion(regionIdNew))
      writeFile(selected.map(_._1), regionIdNew, is, totalLength)
    }
  }

  private def writeFile(clients: Array[FsNodeClient], regionId: Int, is: InputStream, totalLength: Long): FileId = {
    val res = clients.head.writeFile(regionId: Int, is: InputStream, totalLength,
      clients.slice(1, clients.length).map(_.remoteAddress))

    FileId.make(regionId, 0, res)
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

case class RegionStat(regionId: Int, totalSize: Long) {

}

case class NodeStat(maxRegionId: Int, regionCount: Int, sizeMap: Array[RegionStat]) {
}

case class NodeAddress(host: String, port: Int) {

}

class FsNodeClient(val remoteAddress: NodeAddress) extends Logging {
  val endPointRef = {
    val rpcConf = new RpcConf()
    val config = RpcEnvClientConfig(rpcConf, "blobfs-client")
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)

    rpcEnv.setupEndpointRef(RpcAddress(remoteAddress.host, remoteAddress.port), "blobfs-service")
  }

  //10K each chunk
  val CHUNK_SIZE: Int = 10240

  def getNodeStat(): NodeStat = {
    Await.result(endPointRef.ask[GetNodeStatResponse](GetNodeStatRequest()),
      Duration.apply("30s")).nodeStat
  }

  def createRegion(regionId: Int) = {
    Await.result(endPointRef.ask[CreateRegionResponse](CreateRegionRequest(regionId)),
      Duration.apply("30s"))
  }

  def writeFile(regionId: Int, is: InputStream, totalLength: Long, neighbours: Array[NodeAddress]): Int = {
    //small file
    if (totalLength <= CHUNK_SIZE) {
      val bytes = new Array[Byte](CHUNK_SIZE)
      val res = Await.result(endPointRef.ask[SendCompleteFileResponse](
        SendCompleteFileRequest(neighbours, regionId, bytes, totalLength)),
        Duration.apply("30s"))

      res.localId
    }
    else {
      //split files
      val res = Await.result(endPointRef.ask[StartSendChunksResponse](
        StartSendChunksRequest(neighbours, regionId, totalLength)),
        Duration.apply("30s"))

      val transId = res.transId
      var chunks = 0
      var offset = 0
      var n = 0

      val waitLatch = new CountDownLatch(1)
      val results = new ArrayBuffer[Int]()
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
                if (value.localId.isDefined) {
                  results.synchronized {
                    results += value.localId.get
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

      //TODO: how if time out
      if (!waitLatch.await(300, TimeUnit.SECONDS)) {
        throw new WriteFileException(s"time out")
      }

      results.head
    }
  }
}