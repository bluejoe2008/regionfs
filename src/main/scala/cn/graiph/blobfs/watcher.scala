package cn.graiph.blobfs

import cn.graiph.blobfs.util.Logging
import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by bluejoe on 2019/9/3.
  */
class WatchingNodes(zk: ZooKeeper, filter: (NodeAddress) => Boolean) extends Logging {
  val mapNodeClients = mutable.Map[NodeAddress, FsNodeClient]()

  mapNodeClients ++= zk.getChildren(s"/blobfs/nodes", new Watcher {
    private def keepWatching() = {
      zk.getChildren(s"/blobfs/nodes", this.asInstanceOf[Watcher])
    }

    override def process(event: WatchedEvent): Unit = {
      event.getType match {
        case EventType.NodeCreated => {
          val addr = NodeAddress.fromString(event.getPath.drop("/blobfs/nodes".length), "_");
          mapNodeClients += (addr -> FsNodeClient.connect(addr))
          keepWatching
        }

        case EventType.NodeDeleted => {
          val addr = NodeAddress.fromString(event.getPath.drop("/blobfs/nodes".length), "_");
          mapNodeClients(addr).close
          mapNodeClients -= addr
          keepWatching
        }

        case _ => {

        }
      }
    }
  }).map(NodeAddress.fromString(_, "_")).
    filter(filter).
    map { addr =>
      addr -> FsNodeClient.connect(addr)
    }

  logger.debug(s"loaded nodes: ${mapNodeClients.keys}")

  def map = mapNodeClients.toMap

  def isEmpty = mapNodeClients.isEmpty

  def clientOf(addr: NodeAddress): FsNodeClient = mapNodeClients(addr)

  def size = mapNodeClients.size

  def clients = mapNodeClients.values
}

class WatchingRegions(zk: ZooKeeper, filter: (NodeAddress) => Boolean) extends Logging {
  val mapNodeRegions = mutable.Map[NodeAddress, Long]()

  mapNodeRegions ++=
    zk.getChildren(s"/blobfs/regions", new Watcher {

      private def keepWatching() = {
        zk.getChildren(s"/blobfs/regions", this.asInstanceOf[Watcher])
      }

      override def process(event: WatchedEvent): Unit = {
        event.getType match {
          case EventType.NodeCreated => {
            val splits = event.getPath.split("_")
            mapNodeRegions += NodeAddress(splits(0), splits(1).toInt) -> splits(2).toLong
            keepWatching
          }

          case EventType.NodeDeleted => {
            val splits = event.getPath.split("_")
            mapNodeRegions -= NodeAddress(splits(0), splits(1).toInt)
            keepWatching
          }

          case _ => {

          }
        }
      }
    }).map { name =>
      val splits = name.split("_")
      NodeAddress(splits(0), splits(1).toInt) -> splits(2).toLong
    }.
      filter(x => filter(x._1))

  logger.debug(s"loaded neighbour regions: $mapNodeRegions")

  def map = mapNodeRegions.toMap
}