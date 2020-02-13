package cn.graiph.regionfs

/**
  * Created by bluejoe on 2019/8/23.
  */
case class CreateRegionRequest(regionId: Long) {

}

case class CreateRegionResponse(regionId: Long) {

}

case class ListFileRequest() {

}

case class ListFileResponseDetail(result: (FileId, Long)) extends StreamingResult {

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

case class ReadFileRequest(regionId: Long, localId: Long) {

}

case class ReadFileResponseDetail(content: Array[Byte]) extends StreamingResult {

}

case class GetNodeStatRequest() {

}

case class GetNodeStatResponse(stat: NodeStat) {

}

//-------------streaming-------------
case class StartStreamRequest(request: AnyRef, pageSize: Int) {

}

case class StreamResponse(txId: Long, page: Array[_], hasMore: Boolean) {

}

case class GetNextPageRequest(txId: Long) {

}

trait StreamingResult {

}

//-------------streaming-------------