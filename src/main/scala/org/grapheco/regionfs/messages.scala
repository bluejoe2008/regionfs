package org.grapheco.regionfs

import net.neoremind.kraps.rpc.RpcAddress
import org.grapheco.regionfs.client.NodeStat
import org.grapheco.regionfs.server.RegionStatus

/**
  * Created by bluejoe on 2019/8/23.
  */
case class CreateSecondaryRegionRequest(regionId: Long) {

}

case class CreateSecondaryRegionResponse(regionId: Long) {

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

case class CreateSecondaryFileRequest(regionId: Long, localId: Long, totalLength: Long, crc32: Long) {

}

case class CreateFileRequest(totalLength: Long, crc32: Long) {

}

case class CreateFileResponse(nodeId: Int, fileId: FileId, revision: Long) {

}

case class ReadFileRequest(fileId: FileId) {

}

case class DeleteFileRequest(fileId: FileId) {

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