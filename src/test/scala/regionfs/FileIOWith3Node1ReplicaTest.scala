package regionfs

import org.grapheco.regionfs.Constants

/**
  * Created by bluejoe on 2020/3/8.
  */
class FileIOWith3Node1ReplicaTest extends FileIOWith1Node1ReplicaTest {
  override val con = new StrongMultiNode {
    override val GLOBAL_SETTING = Map[String, String](
      Constants.PARAMETER_KEY_ZOOKEEPER_ADDRESS -> zookeeperString,
      Constants.PARAMETER_KEY_REPLICA_NUM -> "1",
      Constants.PARAMETER_KEY_REGION_SIZE_LIMIT -> "9000000",
      Constants.PARAMETER_KEY_REGION_VERSION_CHECK_INTERVAL -> "1000",
      Constants.PARAMETER_KEY_CONSISTENCY_STRATEGY -> "strong",
      Constants.PARAMETER_KEY_REGION_FILE_CLEANUP_INTERVAL -> "1000",
      Constants.PARAMETER_KEY_REGION_MEM_MAX_ALIVE -> "1000"
    )
  }
}
