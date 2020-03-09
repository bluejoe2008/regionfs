package org.grapheco.regionfs

import java.io.{File, FileInputStream}
import java.util.Properties

import org.grapheco.commons.util.ConfigUtils._
import org.grapheco.commons.util.{Configuration, ConfigurationEx}
import org.grapheco.regionfs.util.ZooKeeperClient

import scala.collection.JavaConversions

/**
  * Created by bluejoe on 2020/2/6.
  */
class GlobalConfig(props: Properties) {
  val conf = new Configuration {
    override def getRaw(name: String): Option[String] =
      if (props.containsKey(name)) {
        Some(props.getProperty(name))
      } else {
        None
      }
  }
  lazy val minWritableRegions: Int = conf.get("region.min.writable").withDefault(Constants.DEFAULT_MIN_WRITABLE_REGIONS).asInt
  lazy val replicaNum: Int = conf.get("replica.num").withDefault(1).asInt
  lazy val regionSizeLimit: Long = conf.get("region.size.limit").withDefault(Constants.DEFAULT_REGION_SIZE_LIMIT).asLong
  lazy val enableCrc: Boolean = conf.get("blob.crc.enabled").withDefault(true).asBoolean
  lazy val regionVersionCheckInterval: Long = conf.get("region.version.check.interval").withDefault(Constants.DEFAULT_REGION_VERSION_CHECK_INTERVAL).asLong
}

object GlobalConfig {
  def empty = new GlobalConfig(new Properties())
}

class GlobalConfigWriter {
  def write(props: Properties): Unit = {
    val conf = new ConfigurationEx(props)

    val zks = conf.get("zookeeper.address").asString
    val zk = ZooKeeperClient.create(zks)

    zk.createAbsentNodes();
    zk.saveGlobalConfig(props)

    zk.close()
  }

  def write(map: Map[String, String]): Unit = {
    val props = new Properties();
    props.putAll(JavaConversions.mapAsJavaMap(map))
    write(props)
  }

  def write(configFile: File): Unit = {
    val props = new Properties();
    val fis = new FileInputStream(configFile)
    props.load(fis)
    write(props)
    fis.close()
  }
}