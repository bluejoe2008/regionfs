package cn.bluejoe.regionfs.util

import cn.bluejoe.regionfs.client.{RegionFsException}
import org.apache.zookeeper.ZooKeeper

/**
  * Created by bluejoe on 2020/2/26.
  */
object ZooKeeperUtils {
  def createZookeeperClient(zks: String): ZooKeeper = {
    val zookeeper =
      try {
        new ZooKeeper(zks, 2000, null)
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