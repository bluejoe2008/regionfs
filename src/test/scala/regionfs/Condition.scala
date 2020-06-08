package regionfs

import org.grapheco.regionfs.Constants

/**
  * Created by bluejoe on 2020/3/8.
  */
trait TestCondition {
  val zookeeperString = "localhost:2181"

  val SERVER_NODE_ID: Array[(Int, Int)];

  val GLOBAL_SETTING: Map[String, String];
}

class StrongMultiNode extends TestCondition {
  override val SERVER_NODE_ID = Array(1 -> 1224, 2 -> 1225, 3 -> 1226)
  override val GLOBAL_SETTING = Map[String, String](
    Constants.PARAMETER_KEY_ZOOKEEPER_ADDRESS -> zookeeperString,
    Constants.PARAMETER_KEY_REPLICA_NUM -> "3",
    Constants.PARAMETER_KEY_REGION_SIZE_LIMIT -> "9000000",
    Constants.PARAMETER_KEY_REGION_VERSION_CHECK_INTERVAL -> "1000",
    Constants.PARAMETER_KEY_CONSISTENCY_STRATEGY -> "strong",
    Constants.PARAMETER_KEY_REGION_FILE_CLEANUP_INTERVAL -> "1000",
    Constants.PARAMETER_KEY_REGION_MEM_MAX_ALIVE -> "1000"
  )
}

class EventualMultiNode extends TestCondition {
  override val SERVER_NODE_ID = Array(1 -> 1224, 2 -> 1225, 3 -> 1226)
  override val GLOBAL_SETTING = Map[String, String](
    Constants.PARAMETER_KEY_ZOOKEEPER_ADDRESS -> zookeeperString,
    Constants.PARAMETER_KEY_REPLICA_NUM -> "3",
    Constants.PARAMETER_KEY_REGION_SIZE_LIMIT -> "9000000",
    Constants.PARAMETER_KEY_REGION_VERSION_CHECK_INTERVAL -> "1000",
    Constants.PARAMETER_KEY_CONSISTENCY_STRATEGY -> "eventual",
    Constants.PARAMETER_KEY_REGION_FILE_CLEANUP_INTERVAL -> "1000",
    Constants.PARAMETER_KEY_REGION_MEM_MAX_ALIVE -> "1000"
  )
}

class SingleNode extends TestCondition {
  val SERVER_NODE_ID = Array(1 -> 1224)
  val GLOBAL_SETTING = Map[String, String](
    Constants.PARAMETER_KEY_ZOOKEEPER_ADDRESS -> zookeeperString,
    Constants.PARAMETER_KEY_REPLICA_NUM -> "1",
    Constants.PARAMETER_KEY_REGION_SIZE_LIMIT -> "9000000",
    Constants.PARAMETER_KEY_REGION_VERSION_CHECK_INTERVAL -> "1000",
    Constants.PARAMETER_KEY_REGION_FILE_CLEANUP_INTERVAL -> "1000",
    Constants.PARAMETER_KEY_REGION_MEM_MAX_ALIVE -> "1000"
  )
}