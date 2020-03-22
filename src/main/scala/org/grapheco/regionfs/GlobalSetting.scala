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
class GlobalSetting(props: Properties) {
  val conf = new Configuration {
    override def getRaw(name: String): Option[String] =
      if (props.containsKey(name)) {
        Some(props.getProperty(name))
      } else {
        None
      }
  }

  lazy val consistencyStrategy: Int = conf.get(Constants.PARAMETER_KEY_CONSISTENCY_STRATEGY).
    withDefault(Constants.CONSISTENCY_STRATEGY_STRONG).
    withOptions(Map("strong" -> Constants.CONSISTENCY_STRATEGY_STRONG,
      "eventual" -> Constants.CONSISTENCY_STRATEGY_EVENTUAL)).asInt

  lazy val minWritableRegions: Int = conf.get(Constants.PARAMETER_KEY_MIN_WRITABLE_REGIONS).withDefault(Constants.DEFAULT_MIN_WRITABLE_REGIONS).asInt
  lazy val replicaNum: Int = conf.get(Constants.PARAMETER_KEY_REPLICA_NUM).withDefault(Constants.DEFAULT_REPLICA_NUM).asInt
  lazy val regionSizeLimit: Long = conf.get(Constants.PARAMETER_KEY_REGION_SIZE_LIMIT).withDefault(Constants.DEFAULT_REGION_SIZE_LIMIT).asLong
  lazy val enableCrc: Boolean = conf.get(Constants.PARAMETER_KEY_BLOB_CRC_ENABLED).withDefault(true).asBoolean
  lazy val regionVersionCheckInterval: Long = conf.get(Constants.PARAMETER_KEY_REGION_VERSION_CHECK_INTERVAL).withDefault(
    Constants.DEFAULT_REGION_VERSION_CHECK_INTERVAL).asLong
  lazy val executorThreadPoolSize: Int = conf.get(Constants.PARAMETER_KEY_EXECUTOR_THREAD_POOL_SIZE).withDefault(
    Constants.DEFAULT_EXECUTOR_THREAD_POOL_SIZE).asInt
}

object GlobalSetting {
  def empty = new GlobalSetting(new Properties())
}

class GlobalSettingWriter {
  def write(props: Properties): Unit = {
    val conf = new ConfigurationEx(props)

    val zks = conf.get(Constants.PARAMETER_KEY_ZOOKEEPER_ADDRESS).asString
    val zk = ZooKeeperClient.create(zks)

    zk.createAbsentNodes();
    zk.saveGlobalSetting(props)

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