package cn.bluejoe.regionfs

import cn.bluejoe.regionfs.client.FsNodeClient
import cn.bluejoe.regionfs.util.RpcAddressUtils
import cn.bluejoe.util.Logging
import net.neoremind.kraps.rpc.RpcAddress
import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by bluejoe on 2019/9/3.
  */

/**
  * watches on nodes registered in zookeeper
  * filters node list by parameter filter
  * layout of zookeepper:
  *  /regionfs/nodes
  *    node1_1224
  *    node1_1225
  *    node2_1224
  *    ...
  */
class NodeWatcher(zk: ZooKeeper, filter: (RpcAddress) => Boolean) extends Logging {
  var watchingFlag = true

  def stop(): Unit = {
    watchingFlag = false
    mapNodeClients.foreach(_._2.close())
  }

  //node1_1224->client1, node1_1225->client2, ...
  //client will be automatically created
  val mapNodeClients = mutable.Map[RpcAddress, FsNodeClient]()

  mapNodeClients ++= zk.getChildren(s"/regionfs/nodes", new Watcher {
    private def keepWatching() = {
      if (watchingFlag)
        zk.getChildren(s"/regionfs/nodes", this.asInstanceOf[Watcher])
    }

    override def process(event: WatchedEvent): Unit = {
      event.getType match {
        case EventType.NodeCreated => {
          //get `node1_1224`
          val addr = RpcAddressUtils.fromString(event.getPath.drop("/regionfs/nodes".length), "_");
          //new node created, now add it to mapNodeClients
          mapNodeClients += (addr -> FsNodeClient.connect(addr))
        }

        case EventType.NodeDeleted => {
          val addr = RpcAddressUtils.fromString(event.getPath.drop("/regionfs/nodes".length), "_");
          mapNodeClients(addr).close
          //remove deleted node (dead node)
          mapNodeClients -= addr
        }

        case _ => {

        }
      }

      //keep watching
      //this call renews the getChildren() Events
      keepWatching
    }
  }).map(RpcAddressUtils.fromString(_, "_")).
    filter(filter).
    map { addr =>
      addr -> FsNodeClient.connect(addr)
    }

  logger.debug(s"loaded nodes: ${mapNodeClients.keys}")

  def map = mapNodeClients.toMap

  def isEmpty = mapNodeClients.isEmpty

  def clientOf(addr: RpcAddress): FsNodeClient = mapNodeClients(addr)

  def size = mapNodeClients.size

  def clients = mapNodeClients.values
}

/**
  * watches on regions registered in zookeeper
  * filters region list by parameter filter
  * layout of zookeepper:
  *  /regionfs/regions
  *    node1_1224_1
  *    node1_1225_2
  *    node2_1224_1
  *    ...
  */
class RegionWatcher(zk: ZooKeeper, filter: (RpcAddress) => Boolean) extends Logging {

  //node1_1224->1, node1_1225->2, ...
  val mapNodeRegions = mutable.Map[RpcAddress, Long]()
  var watchingFlag = true

  def stop(): Unit = {
    watchingFlag = false
  }

  mapNodeRegions ++=
    zk.getChildren(s"/regionfs/regions", new Watcher {

      private def keepWatching() = {
        if (watchingFlag)
          zk.getChildren(s"/regionfs/regions", this.asInstanceOf[Watcher])
      }

      override def process(event: WatchedEvent): Unit = {
        event.getType match {
          case EventType.NodeCreated => {
            val splits = event.getPath.split("_")
            mapNodeRegions += RpcAddress(splits(0), splits(1).toInt) -> splits(2).toLong
          }

          case EventType.NodeDeleted => {
            val splits = event.getPath.split("_")
            mapNodeRegions -= RpcAddress(splits(0), splits(1).toInt)
          }

          case _ => {

          }
        }

        keepWatching
      }
    }).map { name =>
      val splits = name.split("_")
      RpcAddress(splits(0), splits(1).toInt) -> splits(2).toLong
    }.
      filter(x => filter(x._1))

  def map = mapNodeRegions.toMap
}


class RegionNodesWatcher(zk: ZooKeeper) extends Logging {
  private val mapRegionNodes = mutable.Map[Long, RpcAddress]()
  // map: Region -> host_port
  var watchingFlag = true

  def stop(): Unit = {
    watchingFlag = false
  }

  mapRegionNodes ++=
    zk.getChildren(s"/regionfs/regions", new Watcher {
      private def keepWatching() = {
        if (watchingFlag)
          zk.getChildren(s"/regionfs/regions", this.asInstanceOf[Watcher])
      }

      override def process(event: WatchedEvent): Unit = {
        event.getType match {
          case EventType.NodeCreated => {
            val splits = event.getPath.split("_")
            mapRegionNodes += splits(2).toLong -> RpcAddress(splits(0), splits(1).toInt)

            keepWatching
          }

          case EventType.NodeDeleted => {
            val splits = event.getPath.split("_")
            mapRegionNodes -= splits(2).toLong

            keepWatching
          }

          case _ => {

          }
        }
      }
    }).map { name =>
      val splits = name.split("_")
      splits(2).toLong -> RpcAddress(splits(0), splits(1).toInt)
    }

  logger.debug(s"loaded neighbour regions: $mapRegionNodes")

  def map = mapRegionNodes.toMap
}