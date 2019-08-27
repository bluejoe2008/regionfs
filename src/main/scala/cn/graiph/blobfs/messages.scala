package cn.graiph.blobfs

/**
  * Created by bluejoe on 2019/8/23.
  */
case class SendCompleteFileRequest(block: Array[Byte], totalLength: Long) {

}

case class SendCompleteFileResponse(fileId: FileId) {
  def regionId = fileId.nodeId;
}

case class DiscardChunksRequest(transId: Int) {

}

case class StartSendChunksRequest(totalLength: Long) {

}

case class StartSendChunksResponse(transId: Int) {

}

case class SendChunkRequest(transId: Int, block: Array[Byte], offset: Long, blockLength: Int, blockIndex: Int) {

}

case class SendChunkResponse(fileId: FileId) {
  def regionId = fileId.nodeId;
}