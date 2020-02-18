package cn.regionfs

import cn.regionfs.util.IteratorUtils

/**
  * Created by bluejoe on 2019/8/31.
  */
//an enhanced FsClient
class FsAdmin(zks: String) extends FsClient(zks: String) {
  def stat(): Stat = {
    Stat(
      nodes.mapNodeClients.values.map(
        _.askSync[GetNodeStatResponse](GetNodeStatRequest()).stat
      ).toList
    )
  }

  def listFiles(): Iterator[(FileId, Long)] = {
    val iter = nodes.mapNodeClients.values.iterator
    IteratorUtils.concatIterators { (index) =>
      if (iter.hasNext) {
        //get 10 files each page
        Some(iter.next().askStream[ListFileResponseDetail](ListFileRequest(), 10).map(_.result))
      }
      else {
        None
      }
    }
  }
}

case class Stat(nodeStats: List[NodeStat]);

case class NodeStat(nodeId: Long, address: NodeAddress, regionStats: List[RegionStat]);

case class RegionStat(regionId: Long, fileCount: Long, totalSize: Long);
