package org.grapheco.regionfs

import net.neoremind.kraps.rpc.RpcAddress
import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}
import org.grapheco.commons.util.Logging
import org.grapheco.regionfs.client.ZooKeeperClient

import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer

/**
  * Created by bluejoe on 2019/9/3.
  */
trait ChildNodeDataAware[T] {
  def onDataModified(t: T, data: Array[Byte]): Unit
}

trait ChildNodePathAware[T] {
  def onCreated(t: T);

  def onDelete(t: T);
}

abstract class ZooKeeperChildrenPathWatcher[T](zk: ZooKeeper, path: String) extends Logging {
  var watchingFlag = true

  def parseChildPath(path: String): T

  def accepts(t: T): Boolean = true

  def stop(onStop: => Unit): Unit = {
    watchingFlag = false
    onStop
  }

  def startWatching(): this.type = {
    val watcher = new Watcher {
      private def keepWatching() = {
        if (watchingFlag)
          zk.getChildren(path, this)
      }

      override def process(event: WatchedEvent): Unit = {
        val cpath = event.getPath
        if (cpath != null) {
          (this, event.getType) match {
            case (cla: ChildNodePathAware[T], EventType.NodeCreated) => {
              if (logger.isTraceEnabled()) {
                logger.trace(s"node created: ${cpath}")
              }

              val t: T = parseChildPath(cpath.drop(cpath.length))

              if (accepts(t))
                cla.onCreated(t)
            }

            case (cla: ChildNodePathAware[T], EventType.NodeDeleted) => {
              if (logger.isTraceEnabled()) {
                logger.trace(s"node deleted: ${cpath}")
              }

              val t: T = parseChildPath(cpath.drop(cpath.length))

              if (accepts(t))
                cla.onDelete(t)
            }

            case (cla: ChildNodeDataAware[T], EventType.NodeDataChanged) => {
              if (logger.isTraceEnabled()) {
                logger.trace(s"node data modified: ${cpath}")
              }

              val t: T = parseChildPath(cpath.drop(cpath.length))
              val data = zk.getData(cpath, false, null);

              if (accepts(t))
                cla.onDataModified(t, data);
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

    this match {
      case cla: ChildNodePathAware[T] =>
        JavaConversions.collectionAsScalaIterable(res).map(parseChildPath).filter(accepts).foreach(t =>
          cla.onCreated(t))
    }

    this
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
abstract class NodeWatcher(zk: ZooKeeperClient)
  extends ZooKeeperChildrenPathWatcher[(Int, RpcAddress)](zk.zookeeper, "/regionfs/nodes")
    with Logging {
  def parseChildPath(path: String): (Int, RpcAddress) = {
    val splits = path.split("_")
    splits(0).toInt -> (RpcAddress(splits(1), splits(2).toInt))
  }

  override def accepts(t: (Int, RpcAddress)): Boolean = true
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
abstract class RegionWatcher(zk: ZooKeeperClient)
  extends ZooKeeperChildrenPathWatcher[(Long, Int)](zk.zookeeper, "/regionfs/regions")
    with Logging {
  def parseChildPath(path: String): (Long, Int) = {
    val splits = path.split("_")
    splits(1).toLong -> splits(0).toInt
  }

  override def accepts(t: (Long, Int)): Boolean = true
}

class Ring[T]() {
  private val _buffer = ArrayBuffer[T]();
  private var pos = 0;

  def -=(t: T) = {
    val idx = _buffer.indexOf(t)
    if (idx != -1) {
      if (idx < pos) {
        pos -= 1
      }
    }
  }

  def +=(t: T) = {
    _buffer += t
  }

  def !(): T = {
    if (pos == _buffer.size)
      pos = 0;

    val t = _buffer(pos)
    pos += 1

    t
  }
}