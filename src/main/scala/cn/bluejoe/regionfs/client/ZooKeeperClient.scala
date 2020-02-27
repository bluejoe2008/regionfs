package cn.bluejoe.regionfs.client

import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}

/**
  * Created by bluejoe on 2020/2/26.
  */
object ZooKeeperClient {
  val NullWatcher = new Watcher {
    override def process(event: WatchedEvent): Unit = {

    }
  }

  def create(zks: String, sessionTimeout: Int = 2000): ZooKeeper = {
    val zookeeper =
      try {
        new ZooKeeper(zks, sessionTimeout, NullWatcher)
      }
      catch {
        case e: Throwable =>
          throw new InvalidZooKeeperConnectionStringException(zks);
      }

    if (zookeeper.exists("/regionfs", false) == null) {
      throw new RegionFsNotInitializedException();
    }

    zookeeper
  }
}

class RegionFsNotInitializedException extends
  RegionFsException(s"RegionFS cluster is not initialized") {

}

class InvalidZooKeeperConnectionStringException(zks: String) extends
  RegionFsException(s"RegionFS cluster is not initialized") {

}