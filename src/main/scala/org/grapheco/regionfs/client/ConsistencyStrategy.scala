package org.grapheco.regionfs.client

import java.io.InputStream
import java.nio.ByteBuffer

import org.grapheco.regionfs.{Constants, FileId}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * Created by bluejoe on 2020/3/12.
  */
trait ConsistencyStrategy {
  def deleteFile(fileId: FileId, regionOwnerNodes: Map[Int, Long]): Future[Boolean]

  def readFile[T](fileId: FileId, regionOwnerNodes: Map[Int, Long], rpcTimeout: Duration): InputStream

  def writeFile(fileIdExpected: FileId, regionOwnerNodes: Map[Int, Long], crc32: Long, content: ByteBuffer): Future[FileId]
}

object ConsistencyStrategy {
  def create(strategyType: Int, clientOf: (Int) => FsNodeClient,
             chooseNextNode: ((Int) => Boolean) => Option[Int]): ConsistencyStrategy =
    strategyType match {
      case Constants.CONSISTENCY_STRATEGY_EVENTUAL =>
        new EventualConsistencyStrategy(clientOf, chooseNextNode)
      case Constants.CONSISTENCY_STRATEGY_STRONG =>
        new StrongConsistencyStrategy(clientOf, chooseNextNode)
    }
}

class EventualConsistencyStrategy(clientOf: (Int) => FsNodeClient, chooseNextNode: ((Int) => Boolean) => Option[Int]) extends ConsistencyStrategy {
  val strongOne = new StrongConsistencyStrategy(clientOf, chooseNextNode);

  def writeFile(fileIdExpected: FileId, regionOwnerNodes: Map[Int, Long], crc32: Long, content: ByteBuffer): Future[FileId] = {
    //only primary region is allowed to write
    clientOf((fileIdExpected.regionId >> 16).toInt).writeFile(fileIdExpected.regionId, crc32, fileIdExpected, content.duplicate())
  }

  def readFile[T](fileId: FileId, regionOwnerNodes: Map[Int, Long], rpcTimeout: Duration): InputStream = {
    //nodes who own newer region
    val maybeNodeId = chooseNextNode(nodeId => regionOwnerNodes.contains(nodeId) && regionOwnerNodes(nodeId) > fileId.localId)
    if (maybeNodeId.isEmpty)
      throw new WrongFileIdException(fileId);

    clientOf(maybeNodeId.get).readFile(fileId, rpcTimeout)
  }

  def deleteFile(fileId: FileId, regionOwnerNodes: Map[Int, Long]): Future[Boolean] = {
    //clientOf((fileId.regionId >> 16).toInt).deleteFile(fileId)
    strongOne.deleteFile(fileId, regionOwnerNodes)
  }
}

class StrongConsistencyStrategy(clientOf: (Int) => FsNodeClient, chooseNextNode: ((Int) => Boolean) => Option[Int]) extends ConsistencyStrategy {
  def writeFile(fileIdExpected: FileId, regionOwnerNodes: Map[Int, Long], crc32: Long, content: ByteBuffer): Future[FileId] = {
    val futures =
      regionOwnerNodes.map(x => clientOf(x._1).writeFile(fileIdExpected.regionId, crc32, fileIdExpected, content.duplicate()))

    Future {
      //TODO: consistency check
      futures.map(x => Await.result(x, Duration.Inf)).head
    }
  }

  def readFile[T](fileId: FileId, regionOwnerNodes: Map[Int, Long], rpcTimeout: Duration): InputStream = {
    val maybeNodeId = chooseNextNode(regionOwnerNodes.contains(_))
    if (maybeNodeId.isEmpty)
      throw new WrongFileIdException(fileId);

    clientOf(maybeNodeId.get).readFile(fileId, rpcTimeout)
  }

  def deleteFile(fileId: FileId, regionOwnerNodes: Map[Int, Long]): Future[Boolean] = {
    //primary node
    clientOf((fileId.regionId >> 16).toInt).deleteFile(fileId)
  }
}