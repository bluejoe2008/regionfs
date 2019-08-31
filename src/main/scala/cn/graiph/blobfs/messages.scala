package cn.graiph.blobfs

/**
  * Created by bluejoe on 2019/8/23.
  */
case class CreateRegionRequest(regionId: Int) {
}

case class CreateRegionResponse(regionId: Int) {
}

case class GetNodeStatRequest() {

}

case class GetNodeStatResponse(nodeStat: NodeStat) {

}

case class SendCompleteFileRequest(neighbours: Array[NodeAddress], regionId: Int, bytes: Array[Byte], totalLength: Long) {

}

case class SendCompleteFileResponse(localId: Int) {
}

case class DiscardChunksRequest(transId: Long) {

}

case class StartSendChunksRequest(neighbours: Array[NodeAddress], regionId: Int, totalLength: Long) {

}

case class StartSendChunksResponse(transId: Long) {

}

case class SendChunkRequest(transId: Long, chunkBytes: Array[Byte], offset: Long, chunkLength: Int, chunkIndex: Int) {

}

case class SendChunkResponse(localId: Option[Int], chunkLength: Int) {

}