package cn.bluejoe.regionfs.client

import cn.bluejoe.regionfs._
import cn.bluejoe.util.IteratorUtils
import net.neoremind.kraps.rpc.RpcAddress

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Created by bluejoe on 2019/8/31.
  */
//an enhanced FsClient
class FsAdmin(zks: String)
  extends FsClient(zks: String) {
  def stat(rpcTimeout: Duration): Stat = {
    Stat {
      val futures = nodes.mapNodeClients.values.map(
        _.endPointRef.ask[GetNodeStatResponse](GetNodeStatRequest()))

      futures.map(x => Await.result(x, rpcTimeout).stat).toList
    }
  }

  def statNode(nodeId: Int, rpcTimeout: Duration): NodeStat = {
    val future = nodes.mapNodeClients(nodeId).endPointRef.ask[GetNodeStatResponse](GetNodeStatRequest())
    Await.result(future, rpcTimeout).stat
  }

  def cleanAllData( rpcTimeout: Duration): Array[RpcAddress] = {
    val futures = nodes.mapNodeClients.values.map(
      _.endPointRef.ask[CleanDataResponse](CleanDataRequest()))

    futures.map(x => Await.result(x, rpcTimeout).address).toArray
  }

  def cleanNodeData(nodeId: Int, rpcTimeout: Duration): RpcAddress = {
    val future = nodes.mapNodeClients(nodeId).endPointRef.ask[CleanDataResponse](CleanDataRequest())
    Await.result(future, Duration.Inf).address
  }

  def shutdownAllNodes( rpcTimeout: Duration): Array[(Int, RpcAddress)] = {
    val res = nodes.mapNodeClients.map(x => x._1 -> x._2.remoteAddress)

    nodes.mapNodeClients.values.map(
      _.endPointRef.ask[ShutdownResponse](ShutdownRequest()))

    res.toArray
  }

  def shutdownNode(nodeId: Int, rpcTimeout: Duration): (Int, RpcAddress) = {
    val client = nodes.mapNodeClients(nodeId)
    client.endPointRef.ask[ShutdownResponse](ShutdownRequest(),rpcTimeout)
    nodeId -> client.remoteAddress
  }

  def greet(nodeId: Int, rpcTimeout: Duration): (Int, RpcAddress) = {
    val future = nodes.mapNodeClients(nodeId).endPointRef.ask[GreetingResponse](GreetingRequest("I am here!!"))
    nodeId -> Await.result(future, rpcTimeout).address
  }

  def listFiles( rpcTimeout: Duration): Iterator[(FileId, Long)] = {
    val iter = nodes.mapNodeClients.values.iterator
    IteratorUtils.concatIterators { (index) =>
      if (iter.hasNext) {
        Some(iter.next().endPointRef.getChunkedStream[ListFileResponseDetail](
          ListFileRequest(), rpcTimeout).map(_.result).iterator)
      }
      else {
        None
      }
    }
  }
}

case class Stat(nodeStats: List[NodeStat]);

case class NodeStat(nodeId: Long, address: RpcAddress, regionStats: List[RegionStat]);

case class RegionStat(regionId: Long, fileCount: Long, totalSize: Long);