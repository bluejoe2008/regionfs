package org.grapheco.regionfs

import net.neoremind.kraps.rpc.RpcAddress
import org.grapheco.regionfs.client.NodeStat
import org.grapheco.regionfs.server.{RegionFileEntry, RegionInfo}

/**
  * Created by bluejoe on 2019/8/23.
  */
case class CreateSecondaryRegionRequest(regionId: Long) {

}

case class CreateSecondaryRegionResponse(info: RegionInfo) {

}

case class CreateSecondaryFilesResponse(info: RegionInfo) {

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

case class CreateFileRequest(totalLength: Long, crc32: Long) {

}

case class CreateFilesRequest(fileCount: Int) {

}

case class CreateFileResponse(fileId: FileId, infos: Array[RegionInfo]) {

}

case class CreateFilesResponse(regionId: Long, fileIds: Array[FileId], infos: Array[RegionInfo]) {

}

case class CreateSecondaryFileRequest(regionId: Long, localId: Long, totalLength: Long, creationTime: Long, crc32: Long) {

}

case class CreateSecondaryFilesRequest(regionId: Long, creationTime: Long, fileCount: Int) {

}

case class CreateSecondaryFileResponse(regionId: Long, localId: Long) {

}

case class MarkSecondaryFileWrittenRequest(regionId: Long, localId: Long, length: Long) {

}

case class MarkSecondaryFilesWrittenRequest(regionId: Long, localIds: Array[Long]) {

}

case class MarkSecondaryFileWrittenResponse(regionId: Long, localId: Long, info: RegionInfo) {

}

case class RegisterSecondaryRegionsRequest(infos: Array[RegionInfo]) {

}

case class ReadFileRequest(fileId: FileId) {

}

case class DeleteFileRequest(fileId: FileId) {

}

case class DeleteSecondaryFileRequest(fileId: FileId) {

}

case class DeleteSecondaryFileResponse(success: Boolean, error: String, info: RegionInfo) {

}

case class DeleteFileResponse(success: Boolean, error: String, infos: Array[RegionInfo]) {

}

case class GetNodeStatRequest() {

}

case class GetNodeStatResponse(stat: NodeStat) {

}

case class GetRegionInfoRequest(regionIds: Array[Long]) {

}

case class GetRegionInfoResponse(infos: Array[RegionInfo]) {

}

case class GetRegionPatchRequest(regionId: Long, since: Long) {

}

case class ReadFileResponseHead(length: Long, crc32: Long, infos: Array[RegionInfo]) {

}

case class GetRegionsOnNodeRequest() {

}

case class GetRegionsOnNodeResponse(infos: Array[RegionInfo]) {

}

case class GetRegionOwnerNodesRequest(regionId: Long) {

}

case class GetRegionOwnerNodesResponse(infos: Array[RegionInfo]) {

}

case class ProcessFilesRequest[T](process: (Iterable[RegionFileEntry]) => T) {

}

case class ProcessFilesResponse[T](value: T) {

}