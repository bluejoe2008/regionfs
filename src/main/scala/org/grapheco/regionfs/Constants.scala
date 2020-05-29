package org.grapheco.regionfs

import org.grapheco.regionfs.util.Version

/**
  * Created by bluejoe on 2020/2/7.
  */
object Constants {
  val CURRENT_VERSION: Version = Version("0.9.2")
  val LOWEST_SUPPORTED_VERSION: Version = Version("0.9.2")

  val FILE_STATUS_TO_WRITE: Byte = 1
  val FILE_STATUS_LOCAL_WRITTEN: Byte = 2
  val FILE_STATUS_GLOBAL_WRITTEN: Byte = 3
  val FILE_STATUS_MERGED: Byte = 4
  val FILE_STATUS_DELETED: Byte = 5

  val DEFAULT_REGION_SIZE_LIMIT: Long = 1024L * 1024 * 1024 * 20
  val DEFAULT_EXECUTOR_THREAD_POOL_SIZE = 20
  val DEFAULT_SERVER_HOST = "localhost"
  val DEFAULT_SERVER_PORT = 1224
  val DEFAULT_WRITE_RETRY_TIMES = 3

  //20G
  val WRITE_CHUNK_SIZE: Int = 1024 * 10
  val READ_CHUNK_SIZE: Int = 1024 * 1024 * 10
  val METADATA_ENTRY_LENGTH_WITH_PADDING = 6 * 8
  val REGION_FILE_ALIGNMENT_SIZE = 8
  val SERVER_SIDE_READ_BUFFER_SIZE = 4096
  val DEFAULT_REGION_VERSION_CHECK_INTERVAL: Long = 60000 * 60
  val DEFAULT_REGION_FILE_CLEANUP_INTERVAL: Long = 60000 * 60
  val DEFAULT_MIN_WRITABLE_REGIONS: Int = 3
  val DEFAULT_REPLICA_NUM: Int = 1
  val MARK_GET_REGION_PATCH_SERVER_IS_BUSY: Byte = 1
  val MARK_GET_REGION_PATCH_ALREADY_UP_TO_DATE: Byte = 2
  val MARK_GET_REGION_PATCH_OK: Byte = 3
  val MAX_BUSY_TRAFFIC: Int = 5
  val MAX_PATCH_SIZE: Long = 100 * 1024 * 1024
  val CONSISTENCY_STRATEGY_EVENTUAL = 1
  val CONSISTENCY_STRATEGY_STRONG = 0

  val PARAMETER_KEY_CONSISTENCY_STRATEGY = "consistency.strategy"
  val PARAMETER_KEY_MIN_WRITABLE_REGIONS = "region.min.writable"
  val PARAMETER_KEY_REPLICA_NUM = "replica.num"
  val PARAMETER_KEY_REGION_SIZE_LIMIT = "region.size.limit"
  val PARAMETER_KEY_REGION_VERSION_CHECK_INTERVAL = "region.version.check.interval"
  val PARAMETER_KEY_REGION_FILE_CLEANUP_INTERVAL = "region.file.cleanup.interval"
  val PARAMETER_KEY_EXECUTOR_THREAD_POOL_SIZE = "executor.thread.pool.size"
  val PARAMETER_KEY_ZOOKEEPER_ADDRESS = "zookeeper.address"
  val PARAMETER_KEY_NODE_ID = "node.id"
  val PARAMETER_KEY_SERVER_HOST = "server.host"
  val PARAMETER_KEY_SERVER_PORT = "server.port"
  val PARAMETER_KEY_DATA_STORE_DIR = "data.storeDir"
  val PARAMETER_KEY_WRITE_RETRY_TIMES = "write.retry.times"
}
