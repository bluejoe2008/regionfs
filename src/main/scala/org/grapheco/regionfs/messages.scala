package org.grapheco.regionfs

import net.neoremind.kraps.rpc.RpcAddress
import org.grapheco.regionfs.client.NodeStat

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

case class GreetingRequest(msg: String) {

}

case class GreetingResponse(address: RpcAddress) {

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

case class DeleteFileRequest(regionId: Long, localId: Long) {

}

case class DeleteFileResponse(success: Boolean, error: String) {

}

case class GetNodeStatRequest() {

}

case class GetNodeStatResponse(stat: NodeStat) {

}