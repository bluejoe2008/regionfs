package cn.bluejoe.regionfs

import java.io.{ByteArrayInputStream, File, FileInputStream}
import java.util.Properties
import cn.bluejoe.regionfs.server.RegionFsServersException
import cn.bluejoe.regionfs.util.ConfigurationEx
import org.apache.commons.io.IOUtils
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}

/**
  * Created by bluejoe on 2020/2/6.
  */
case class GlobalConfig(replicaNum: Int, regionSizeLimit: Long, enableCrc: Boolean) {
}

object GlobalConfig {
  def load(zk: ZooKeeper): GlobalConfig = {
    if (zk.exists("/regionfs/config", null) == null) {
      throw new GlobalConfigPathNotFoundException("/regionfs/config");
    }
    val bytes = zk.getData("/regionfs/config", null, null)
    val bais = new ByteArrayInputStream(bytes);
    val props = new Properties()
    props.load(bais)

    val conf = new ConfigurationEx(props)

    new GlobalConfig(conf.get("replica.num").withDefault(3).asInt,
      conf.get("region.size.limit").withDefault(Constants.DEFAULT_REGION_SIZE_LIMIT).asLong,
      conf.get("blob.crc.enabled").withDefault(true).asBoolean)
  }

  def save(zk: ZooKeeper, bytes: Array[Byte]): Unit = {
    if (zk.exists("/regionfs/config", null) == null) {
      zk.create("/regionfs/config", bytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    }
    else {
      zk.setData("/regionfs/config", bytes, -1)
    }
  }
}

class GlobalConfigPathNotFoundException(path: String) extends
  RegionFsServersException(s"zookeeper path not exists: $path") {

}

class GlobalConfigConfigurer {
  def config(configFile: File): Unit = {
    val conf = new ConfigurationEx(configFile)

    val zks = conf.get("zookeeper.address").asString
    val zk = new ZooKeeper(zks, 2000, new Watcher {
      override def process(event: WatchedEvent): Unit = {
      }
    })

    val fis = new FileInputStream(configFile)
    GlobalConfig.save(zk, IOUtils.toByteArray(fis))
    fis.close()

    zk.close()
  }
}