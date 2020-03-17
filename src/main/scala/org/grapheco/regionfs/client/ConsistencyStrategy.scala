package org.grapheco.regionfs.client

import java.io.InputStream
import java.nio.ByteBuffer

import org.grapheco.regionfs.{Constants, FileId}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration

/**
  * Created by bluejoe on 2020/3/12.
  */
trait ConsistencyStrategy {
  def deleteFile(fileId: FileId, regionOwnerNodes: Map[Int, Long]): Future[Boolean]

  def readFile[T](fileId: FileId, regionOwnerNodes: Map[Int, Long], rpcTimeout: Duration): InputStream

  def writeFile(chosedNodeId: Int, crc32: Long, content: ByteBuffer): Future[FileId]
}

object ConsistencyStrategy {
  def create(strategyType: Int, clientOf: (Int) => FsNodeClient,
             chooseNextNode: ((Int) => Boolean) => Option[Int],
             updateLocalRegionCache: (Long, Array[(Int, Long)]) => Unit): ConsistencyStrategy =
    strategyType match {
      case Constants.CONSISTENCY_STRATEGY_EVENTUAL =>
        new EventualConsistencyStrategy(clientOf, chooseNextNode, updateLocalRegionCache)
      case Constants.CONSISTENCY_STRATEGY_STRONG =>
        new StrongConsistencyStrategy(clientOf, chooseNextNode, updateLocalRegionCache)
    }
}

class EventualConsistencyStrategy(clientOf: (Int) => FsNodeClient,
                                  chooseNextNode: ((Int) => Boolean) => Option[Int],
                                  updateLocalRegionCache: (Long, Array[(Int, Long)]) => Unit) extends ConsistencyStrategy {
  val strongOne = new StrongConsistencyStrategy(clientOf, chooseNextNode, updateLocalRegionCache);

  def writeFile(chosedNodeId: Int, crc32: Long, content: ByteBuffer): Future[FileId] = {
    strongOne.writeFile(chosedNodeId, crc32: Long, content)
  }

  def readFile[T](fileId: FileId, regionOwnerNodes: Map[Int, Long], rpcTimeout: Duration): InputStream = {
    //nodes who own newer region
    val maybeNodeId =
      chooseNextNode(nodeId =>
        regionOwnerNodes.contains(nodeId) && regionOwnerNodes(nodeId) > fileId.localId)

    if (maybeNodeId.isEmpty)
      throw new WrongFileIdException(fileId);
    clientOf(maybeNodeId.get).readFile(fileId, rpcTimeout)
  }

  def deleteFile(fileId: FileId, regionOwnerNodes: Map[Int, Long]): Future[Boolean] = {
    strongOne.deleteFile(fileId, regionOwnerNodes)
  }
}

class StrongConsistencyStrategy(clientOf: (Int) => FsNodeClient,
                                chooseNextNode: ((Int) => Boolean) => Option[Int],
                                updateLocalRegionCache: (Long, Array[(Int, Long)]) => Unit) extends ConsistencyStrategy {
  def writeFile(chosedNodeId: Int, crc32: Long, content: ByteBuffer): Future[FileId] = {
    //only primary region is allowed to write
    clientOf(chosedNodeId).createFile(crc32, content.duplicate()).map(x => {
      updateLocalRegionCache(x._1.regionId, x._2)
      x._1
    })
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