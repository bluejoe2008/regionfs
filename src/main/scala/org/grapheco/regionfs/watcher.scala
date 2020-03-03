package org.grapheco.regionfs

import net.neoremind.kraps.rpc.RpcAddress
import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}
import org.grapheco.commons.util.Logging

import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer

/**
  * Created by bluejoe on 2019/9/3.
  */
abstract class ZooKeeperPathWatcher[T](zk: ZooKeeper, path: String) extends Logging {
  var watchingFlag = true

  def parseChildPath(path: String): T

  def accepts(t: T): Boolean

  def onCreated(t: T);

  def onDelete(t: T);

  def stop(onStop: => Unit): Unit = {
    watchingFlag = false
    onStop
  }

  def startWatching(): this.type = {
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
              val t: T = parseChildPath(path.drop(path.length))

              if (accepts(t))
                onCreated(t)
            }

            case EventType.NodeDeleted => {
              val t: T = parseChildPath(path.drop(path.length))

              if (accepts(t))
                onDelete(t)
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
    JavaConversions.collectionAsScalaIterable(res).map(parseChildPath).filter(accepts).foreach(t =>
      onCreated(t))

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
abstract class NodeWatcher(zk: ZooKeeper) extends ZooKeeperPathWatcher[(Int, RpcAddress)](zk, "/regionfs/nodes") with Logging {
  def parseChildPath(path: String): (Int, RpcAddress) = {
    val splits = path.split("_")
    splits(0).toInt -> (RpcAddress(splits(1), splits(2).toInt))
  }

  def accepts(t: (Int, RpcAddress)): Boolean = true
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
abstract class RegionWatcher(zk: ZooKeeper) extends ZooKeeperPathWatcher[(Long, Int)](zk, "/regionfs/regions") with Logging {
  def parseChildPath(path: String): (Long, Int) = {
    val splits = path.split("_")
    splits(1).toLong -> splits(0).toInt
  }

  def accepts(t: (Long, Int)): Boolean = true
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