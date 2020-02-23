package cn.regionfs

import cn.regionfs.client.NodeStat

/**
  * Created by bluejoe on 2019/8/23.
  */
case class CreateRegionRequest(regionId: Long) {

}

case class CreateRegionResponse(regionId: Long) {

}

case class ListFileRequest() {

}

case class ListFileResponseDetail(result: (FileId, Long)) {

}

case class SendFileRequest(maybeRegionId: Option[Long], totalLength: Long) {

}

case class SendFileResponse(fileId: FileId) {

}

case class ReadFileRequest(regionId: Long, localId: Long) {

}

case class GetNodeStatRequest() {

}

case class GetNodeStatResponse(stat: NodeStat) {

}