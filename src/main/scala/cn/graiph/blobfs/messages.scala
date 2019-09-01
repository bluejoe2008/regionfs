package cn.graiph.blobfs

/**
  * Created by bluejoe on 2019/8/23.
  */
case class CreateRegionRequest(regionId: Int) {
}

case class CreateRegionResponse(regionId: Int) {
}

case class SendCompleteFileRequest(regionId: Option[Int], bytes: Array[Byte], totalLength: Long) {

}

case class SendCompleteFileResponse(fileId: FileId) {
}

case class DiscardChunksRequest(transId: Long) {

}

case class StartSendChunksRequest(regionId: Option[Int], totalLength: Long) {

}

case class StartSendChunksResponse(transId: Long) {

}

case class SendChunkRequest(transId: Long, chunkBytes: Array[Byte], offset: Long, chunkLength: Int, chunkIndex: Int) {

}

case class SendChunkResponse(fileId: Option[FileId], chunkLength: Int) {

}