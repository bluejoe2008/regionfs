package cn.graiph.regionfs

/**
  * Created by bluejoe on 2019/8/31.
  */
//an enhanced FsClient
class FsAdmin(zks: String) extends FsClient(zks: String) {
  def stat(): Stat = {
    Stat(
      nodes.mapNodeClients.values.map(
        _.ask[GetNodeStatResponse](GetNodeStatRequest()).stat
      ).toList
    )
  }

  def listFiles(): Iterator[(FileId, Long)] = {
    nodes.mapNodeClients.values.flatMap(
      _.ask[ListFileResponse](ListFileRequest()).list).iterator
  }
}

case class Stat(nodeStats: List[NodeStat]);

case class NodeStat(nodeId: Long, address: NodeAddress, regionStats: List[RegionStat]);

case class RegionStat(regionId: Long, fileCount: Long, totalSize: Long);
