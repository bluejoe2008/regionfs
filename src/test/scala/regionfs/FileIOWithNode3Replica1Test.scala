package regionfs

/**
  * Created by bluejoe on 2020/3/8.
  */
class FileIOWithNode3Replica1Test extends FileIOWithNode1Test {
  override val con = new StrongMultiNode {
    override val GLOBAL_SETTING = Map[String, String](
      "zookeeper.address" -> zookeeperString,
      "replica.num" -> "1",
      "region.size.limit" -> "9000000",
      "blob.crc.enabled" -> "true",
      "region.version.check.interval" -> "1000",
      "consistency.strategy" -> "strong"
    )
  }
}
