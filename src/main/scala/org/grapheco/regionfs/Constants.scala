package org.grapheco.regionfs

import org.grapheco.regionfs.util.Version

/**
  * Created by bluejoe on 2020/2/7.
  */
object Constants {
  val CURRENT_VERSION: Version = Version("0.9.2")
  val LOWEST_SUPPORTED_VERSION: Version = Version("0.9.2")

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

  val DEFAULT_REGION_MEM_DIRTY_TIMEOUT: Long = 10 * 60 * 1000
  val DEFAULT_REGION_MEM_ENTRY_COUNT: Int = 50
  val DEFAULT_REGION_MEM_SIZE: Long = 1 * 1024 * 1024
  val DEFAULT_MAX_WRITE_FILE_BATCH_SIZE: Long = 2 * 1024 * 1024
  val PARAMETER_KEY_REGION_MEM_MAX_ALIVE = "region.mem.max_alive"
  val PARAMETER_KEY_REGION_MEM_MAX_ENTRY_COUNT = "region.mem.max_entry_count"
  val PARAMETER_KEY_REGION_MEM_MAX_TOTAL_SIZE = "region.mem.max_total_size"
  val PARAMETER_KEY_MAX_WRITE_FILE_BATCH_SIZE = "write.max_batch_size"
  val PARAMETER_KEY_CONSISTENCY_STRATEGY = "consistency.strategy"
  val PARAMETER_KEY_MIN_WRITABLE_REGIONS = "region.min_writable"
  val PARAMETER_KEY_REPLICA_NUM = "region.replica_num"
  val PARAMETER_KEY_REGION_SIZE_LIMIT = "region.max_size"
  val PARAMETER_KEY_REGION_VERSION_CHECK_INTERVAL = "region.version_check_interval"
  val PARAMETER_KEY_REGION_FILE_CLEANUP_INTERVAL = "region.cleanup_interval"
  val PARAMETER_KEY_EXECUTOR_THREAD_POOL_SIZE = "executor.thread_pool_size"
  val PARAMETER_KEY_WRITE_RETRY_TIMES = "write.max_retry_times"
  val PARAMETER_KEY_ZOOKEEPER_ADDRESS = "zookeeper.address"
  val PARAMETER_KEY_NODE_ID = "node.id"
  val PARAMETER_KEY_SERVER_HOST = "server.host"
  val PARAMETER_KEY_SERVER_PORT = "server.port"
  val PARAMETER_KEY_DATA_STORE_DIR = "data.store_dir"
}
