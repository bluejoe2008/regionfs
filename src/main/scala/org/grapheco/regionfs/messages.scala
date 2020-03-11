package org.grapheco.regionfs

import net.neoremind.kraps.rpc.RpcAddress
import org.grapheco.regionfs.client.NodeStat
import org.grapheco.regionfs.server.RegionStatus

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

case class SendFileRequest(regionId: Long, fileId: FileId, totalLength: Long, crc32: Long) {

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

case class PrepareToWriteFileRequest(fileSize: Long) {

}

case class PrepareToWriteFileResponse(regionId: Long, fileId: FileId, regionOwnerNodes: Array[Int]) {

}

case class GetRegionStatusRequest(regionIds: Array[Long]) {

}

case class GetRegionStatusResponse(statusList: Array[RegionStatus]) {

}

case class GetRegionPatchRequest(regionId: Long, since: Long) {

}