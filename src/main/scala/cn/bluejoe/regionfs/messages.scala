package cn.bluejoe.regionfs

import cn.bluejoe.regionfs.client.NodeStat
import net.neoremind.kraps.rpc.RpcAddress

/**
  * Created by bluejoe on 2019/8/23.
  */
case class CreateRegionRequest(regionId: Long) {

}

case class CreateRegionResponse(regionId: Long) {

}

case class ShutdownRequest() {

}

case class ShutdownResponse(address: RpcAddress) {

}

case class CleanDataRequest() {

}

case class CleanDataResponse(address: RpcAddress) {

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