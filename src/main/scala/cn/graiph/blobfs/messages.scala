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

case class GetNodeStatResponse(nodeStat: NodeStat){

}

case class SendCompleteFileRequest(neighbours: Array[NodeAddress], regionId: Int, block: Array[Byte], totalLength: Long) {

}

case class SendCompleteFileResponse(localId: Int) {
}

case class DiscardChunksRequest(transId: Int) {

}

case class StartSendChunksRequest(neighbours: Array[NodeAddress], regionId: Int, totalLength: Long) {

}

case class StartSendChunksResponse(transId: Int) {

}

case class SendChunkRequest(transId: Int, block: Array[Byte], offset: Long, blockLength: Int, blockIndex: Int) {

}

case class SendChunkResponse(localId: Int) {

}