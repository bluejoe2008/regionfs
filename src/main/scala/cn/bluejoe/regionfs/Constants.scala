package cn.bluejoe.regionfs

/**
  * Created by bluejoe on 2020/2/7.
  */
object Constants {
  val DEFAULT_REGION_SIZE_LIMIT = 1024L * 1024 * 1024 * 20 //20G
  val WRITE_CHUNK_SIZE: Int = 1024 * 10
  val READ_CHUNK_SIZE: Int = 1024 * 1024 * 10
  val METADATA_ENTRY_LENGTH_WITH_PADDING = 40
  val REGION_FILE_BODY_EOF: Array[Byte] = "\r\n----\r\n".getBytes
  val SERVER_SIDE_READ_BUFFER_SIZE = 4096;
}
