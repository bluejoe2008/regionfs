package org.grapheco.regionfs

import net.neoremind.kraps.rpc.RpcAddress
import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}
import org.grapheco.commons.util.Logging
import org.grapheco.regionfs.client.{FsNodeClient, FsNodeClientFactory}

import scala.collection.mutable.ArrayBuffer
import scala.collection.{JavaConversions, mutable}

/**
  * Created by bluejoe on 2019/9/3.
  */

class ZooKeeperPathWatcher(zk: ZooKeeper) extends Logging {
  var watchingFlag = true

  def stop(onStop: => Unit): Unit = {
    watchingFlag = false
    onStop
  }

  def startWatching[T](
                        path: String,
                        childPathParser: (String) => T,
                        filter: (T) => Boolean,
                        onStarted: (Iterable[T]) => Unit,
                        onNodeCreated: (T) => Unit,
                        onNodeDeleted: (T) => Unit) {
    val watcher = new Watcher {
      private def keepWatching() = {
        if (watchingFlag)
          zk.getChildren(path, this.asInstanceOf[Watcher])
      }

      override def process(event: WatchedEvent): Unit = {
        val path = event.getPath
        if (path != null) {
          event.getType match {
            case EventType.NodeCreated => {
              val t: T = childPathParser(path.drop(path.length))

              if (filter(t))
                onNodeCreated(t)
            }

            case EventType.NodeDeleted => {
              val t: T = childPathParser(path.drop(path.length))

              if (filter(t))
                onNodeDeleted(t)
            }

            case _ => {
            }
          }
        }

        //keep watching
        //this call renews the getChildren() Events
        keepWatching
      }
    }

    val res = zk.getChildren(path, watcher)
    onStarted(JavaConversions.collectionAsScalaIterable(res).map(childPathParser).filter(filter))
  }
}

/**
  * watches on nodes registered in zookeeper
  * filters node list by parameter filter
  * layout of zookeepper:
  *  /regionfs/nodes
  *    1_192.168.100.1_1224
  *    2_192.168.100.1_1225
  *    3_192.168.100.2_1224
  *    ...
  */
class UpdatingNodeList(clientFactory: FsNodeClientFactory, zk: ZooKeeper, nodeFilter: (Int => Boolean)) extends Logging {
  var watcher = new ZooKeeperPathWatcher(zk)

  def stop(): Unit = {
    watcher.stop()
  }

  //1->client1, 2->client2, ...
  //client will be automatically created
  val mapNodeClients = mutable.Map[Int, FsNodeClient]()

  def start(): UpdatingNodeList = {
    watcher.startWatching[(Int, RpcAddress)](s"/regionfs/nodes",
      childPathParser = (path: String) => {
        val splits = path.split("_")
        splits(0).toInt -> (RpcAddress(splits(1), splits(2).toInt))
      },
      filter = (kv: (Int, RpcAddress)) => {
        nodeFilter(kv._1)
      },
      onStarted = (list: Iterable[(Int, RpcAddress)]) => {
        mapNodeClients ++= list.map {
          kv =>
            kv._1 -> clientFactory.of(kv._2)
        }
      },
      onNodeCreated = (kv: (Int, RpcAddress)) => {
        mapNodeClients += kv._1 -> (clientFactory.of(kv._2))
      },
      onNodeDeleted = (kv: (Int, RpcAddress)) => {
        mapNodeClients -= kv._1
      })

    this;
  }
}

/**
  * watches on regions registered in zookeeper
  * filters region list by parameter filter
  * layout of zookeepper:
  *  /regionfs/regions
  *    1_32768
  *    1_32769
  *    2_65536
  *    ...
  */
class UpdatingRegionList(clientFactory: FsNodeClientFactory, zk: ZooKeeper, nodeFilter: (Int) => Boolean) extends Logging {

  //32768->(1,2), 32769->(1), ...
  val mapRegionNodes = mutable.Map[Long, ArrayBuffer[Int]]()
  val mapNodeRegionCount = mutable.Map[Int, Int]()

  var watcher = new ZooKeeperPathWatcher(zk)

  def stop(): Unit = {
    watcher.stop()
  }

  def start(): UpdatingRegionList = {
    watcher.startWatching[(Long, Int)](s"/regionfs/regions",
      childPathParser = (path: String) => {
        val splits = path.split("_")
        splits(1).toLong -> splits(0).toInt
      },
      filter = (kv: (Long, Int)) => {
        nodeFilter(kv._2)
      },
      onStarted = (list: Iterable[(Long, Int)]) => {
        //32768->1, 32769->1
        mapRegionNodes ++= (
          list.groupBy(_._1).map(x =>
            x._1 -> (ArrayBuffer() ++ x._2.map(_._2)))
          )
        mapNodeRegionCount ++= list.groupBy(_._2).map(x => x._1 -> x._2.size)
      },
      onNodeCreated = (kv: (Long, Int)) => {
        mapNodeRegionCount.update(kv._2, mapNodeRegionCount.getOrElse(kv._2, 0) + 1)
        mapRegionNodes.getOrElse(kv._1, ArrayBuffer()) += kv._2
      },
      onNodeDeleted = (kv: (Long, Int)) => {
        mapRegionNodes -= kv._1
        mapNodeRegionCount.update(kv._2, mapNodeRegionCount(kv._2) - 1)
        mapRegionNodes(kv._1) -= kv._2
      })

    this;
  }
}