package org.grapheco.regionfs.client

import net.neoremind.kraps.rpc.RpcAddress
import org.grapheco.commons.util.IteratorUtils
import org.grapheco.regionfs._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Created by bluejoe on 2019/8/31.
  */
//an enhanced FsClient
class FsAdmin(zks: String) extends FsClient(zks: String) {
  def stat(rpcTimeout: Duration): Stat = {
    Stat {
      val futures = mapNodeWithAddress.map(x =>
        clientOf(x._1).endPointRef.ask[GetNodeStatResponse](GetNodeStatRequest()))

      futures.map(x => Await.result(x, rpcTimeout).stat).toList
    }
  }

  def getNodesWithAddress(): Map[Int, RpcAddress] = {
    mapNodeWithAddress.toMap
  }

  def getRegionsWithListOfNodes(): Map[Long, Array[Int]] = {
    mapRegionWithNodes.map(x => x._1 -> x._2.toArray).toMap
  }

  def getRegions(): Iterable[Long] = {
    mapRegionWithNodes.keys
  }

  def getRegions(nodeId: Int): Iterable[Long] = {
    arrayRegionWithNode.filter(_._2 == nodeId).map(_._1)
  }

  def getNodes(regionId: Long): Iterable[Int] = {
    mapRegionWithNodes(regionId)
  }

  def getNodes(): Iterable[Int] = {
    mapNodeWithAddress.keys
  }

  def statNode(nodeId: Int, rpcTimeout: Duration): NodeStat = {
    val future = clientOf(nodeId).endPointRef.ask[GetNodeStatResponse](GetNodeStatRequest())
    Await.result(future, rpcTimeout).stat
  }

  def cleanAllData(rpcTimeout: Duration): Array[RpcAddress] = {
    val futures = mapNodeWithAddress.map(x =>
      clientOf(x._1).endPointRef.ask[CleanDataResponse](CleanDataRequest()))

    futures.map(x => Await.result(x, rpcTimeout).address).toArray
  }

  def cleanNodeData(nodeId: Int, rpcTimeout: Duration): RpcAddress = {
    val future = clientOf(nodeId).endPointRef.ask[CleanDataResponse](CleanDataRequest())
    Await.result(future, Duration.Inf).address
  }

  def shutdownAllNodes(rpcTimeout: Duration): Array[(Int, RpcAddress)] = {
    mapNodeWithAddress.map(x =>
      clientOf(x._1).endPointRef.ask[ShutdownResponse](ShutdownRequest()))

    mapNodeWithAddress.toArray
  }

  def shutdownNode(nodeId: Int, rpcTimeout: Duration): (Int, RpcAddress) = {
    val client = clientOf(nodeId)
    client.endPointRef.ask[ShutdownResponse](ShutdownRequest(), rpcTimeout)
    nodeId -> client.remoteAddress
  }

  def greet(nodeId: Int, rpcTimeout: Duration): (Int, RpcAddress) = {
    val future = clientOf(nodeId).endPointRef.ask[GreetingResponse](GreetingRequest("I am here!!"))
    nodeId -> Await.result(future, rpcTimeout).address
  }

  def listFiles(rpcTimeout: Duration): Iterator[(FileId, Long)] = {
    val iter = mapNodeWithAddress.iterator
    IteratorUtils.concatIterators { (index) =>
      if (iter.hasNext) {
        Some(clientOf(iter.next()._1).endPointRef.getChunkedStream[ListFileResponseDetail](
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