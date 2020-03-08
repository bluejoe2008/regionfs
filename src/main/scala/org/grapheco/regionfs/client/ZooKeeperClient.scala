package org.grapheco.regionfs.client

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.Properties

import net.neoremind.kraps.rpc.RpcAddress
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}
import org.grapheco.regionfs.GlobalConfig
import org.grapheco.regionfs.server.{ExisitingNodeInZooKeeperExcetion, Region, RegionData, RegionFsServerException}

/**
  * Created by bluejoe on 2020/2/26.
  */
object ZooKeeperClient {
  val NullWatcher = new Watcher {
    override def process(event: WatchedEvent): Unit = {

    }
  }

  def create(zks: String, sessionTimeout: Int = 2000): ZooKeeperClient = {
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

    new ZooKeeperClient(zookeeper)
  }
}

class ZooKeeperClient(val zookeeper: ZooKeeper) {
  def createAbsentNodes() {
    if (zookeeper.exists("/regionfs", false) == null)
      zookeeper.create("/regionfs", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)

    if (zookeeper.exists("/regionfs/nodes", false) == null)
      zookeeper.create("/regionfs/nodes", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)

    if (zookeeper.exists("/regionfs/regions", false) == null)
      zookeeper.create("/regionfs/regions", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
  }

  def readRegionData(regionId: Long): RegionData = {
    val nodeId = regionId >> 16;
    val data = zookeeper.getData(s"/regionfs/regions/${nodeId}_${regionId}", false, null)
    Region.unpack(nodeId, regionId, data);
  }

  def writeRegionData(nodeId: Int, region: Region) = {
    zookeeper.setData(s"/regionfs/regions/${nodeId}_${region.regionId}",
      Region.pack(region), -1)
  }

  def assertPathNotExists(path: String)(onAssertFailed: => Unit) = {
    if (zookeeper.exists(path, ZooKeeperClient.NullWatcher) != null) {
      onAssertFailed
      throw new ExisitingNodeInZooKeeperExcetion(path);
    }
  }

  def createRegionNode(nodeId: Int, region: Region) = {
    zookeeper.create(s"/regionfs/regions/${nodeId}_${region.regionId}",
      Region.pack(region), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
  }

  def createNodeNode(nodeId: Int, address: RpcAddress) = {
    zookeeper.create(s"/regionfs/nodes/${nodeId}_${address.host}_${address.port}", "".getBytes,
      Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
  }

  def close(): Unit = {
    zookeeper.close()
  }

  def loadGlobalConfig(): GlobalConfig = {
    if (zookeeper.exists("/regionfs/config", null) == null) {
      throw new GlobalConfigPathNotFoundException("/regionfs/config");
    }

    val bytes = zookeeper.getData("/regionfs/config", null, null)
    val bais = new ByteArrayInputStream(bytes);
    val props = new Properties()
    props.load(bais)

    new GlobalConfig(props)
  }

  def saveGlobalConfig(props: Properties): Unit = {
    val baos = new ByteArrayOutputStream();
    props.store(baos, "global setting of region-fs")
    val bytes = baos.toByteArray

    if (zookeeper.exists("/regionfs/config", null) == null) {
      zookeeper.create("/regionfs/config", bytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    }
    else {
      zookeeper.setData("/regionfs/config", bytes, -1)
    }
  }
}

class RegionFsNotInitializedException extends
  RegionFsException(s"RegionFS cluster is not initialized") {

}

class InvalidZooKeeperConnectionStringException(zks: String) extends
  RegionFsException(s"RegionFS cluster is not initialized") {

}

class GlobalConfigPathNotFoundException(path: String) extends
  RegionFsServerException(s"zookeeper path not exists: $path") {

}
