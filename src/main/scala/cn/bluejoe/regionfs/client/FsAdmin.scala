package cn.bluejoe.regionfs.client

import java.io.{FileInputStream, File}

import cn.bluejoe.regionfs.util.ConfigurationEx
import cn.bluejoe.util.IteratorUtils
import cn.bluejoe.regionfs._
import net.neoremind.kraps.rpc.RpcAddress

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Created by bluejoe on 2019/8/31.
  */
//an enhanced FsClient
class FsAdmin(zks: String) extends FsClient(zks: String) {
  def stat(): Stat = {
    Stat(
      nodes.mapNodeClients.values.map(x =>
        Await.result(x.endPointRef.ask[GetNodeStatResponse](GetNodeStatRequest()), Duration.Inf).stat
      ).toList
    )
  }

  def listFiles(): Iterator[(FileId, Long)] = {
    val iter = nodes.mapNodeClients.values.iterator
    IteratorUtils.concatIterators { (index) =>
      if (iter.hasNext) {
        //get 10 files each page
        Some(iter.next().endPointRef.getChunkedStream[ListFileResponseDetail](ListFileRequest()).map(_.result).iterator)
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