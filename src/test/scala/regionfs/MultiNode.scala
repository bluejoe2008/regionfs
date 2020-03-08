package regionfs

import java.io.File

import org.apache.commons.io.FileUtils

/**
  * Created by bluejoe on 2020/3/8.
  */
trait MultiNode extends SingleNode {
  override val SERVER_NODE_ID = Array[(Int, Int)]()
  //override val SERVER_NODE_ID = Array(1 -> 1224, 2 -> 1225, 3 -> 1226)
  override val GLOBAL_SETTING = Map[String, String](
    "zookeeper.address" -> zookeeperString,
    "replica.num" -> "3",
    "region.size.limit" -> "100000000",
    "blob.crc.enabled" -> "true",
    "region.version.check.interval" -> "1000"
  )

  StartNodeServerForTest.zookeeperString
}

trait SingleNode {
  val zookeeperString = "localhost:2181"
  FileUtils.deleteDirectory(new File("./testdata/nodes"));

  val SERVER_NODE_ID = Array(1 -> 1224)
  val GLOBAL_SETTING = Map[String, String](
    "zookeeper.address" -> zookeeperString,
    "replica.num" -> "1",
    "region.size.limit" -> "100000000",
    "blob.crc.enabled" -> "true",
    "region.version.check.interval" -> "1000"
  )

}