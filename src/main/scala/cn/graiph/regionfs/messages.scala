package cn.graiph.regionfs

/**
  * Created by bluejoe on 2019/8/23.
  */
case class CreateRegionRequest(regionId: Long) {

}

case class CreateRegionResponse(regionId: Long) {

}

case class SendCompleteFileRequest(regionId: Option[Long], bytes: Array[Byte], totalLength: Long) {

}

case class SendCompleteFileResponse(fileId: FileId) {

}

case class DiscardSendChunksRequest(transId: Long) {

}

case class StartSendChunksRequest(regionId: Option[Long], totalLength: Long) {

}

case class StartSendChunksResponse(transId: Long) {

}

case class SendChunkRequest(transId: Long, chunkBytes: Array[Byte], offset: Long, chunkLength: Int, chunkIndex: Int) {

}

case class SendChunkResponse(fileId: Option[FileId], chunkLength: Long) {

}

case class ReadChunkRequest(regionId: Long, localId: Long, offset: Long, chunkLength: Long) {

}

case class ReadChunkResponse(content: Array[Byte], nextOffset: Long) {

}