package org.grapheco.regionfs.util

import java.io._
import java.util.Properties
import java.util.concurrent.Executors

import net.neoremind.kraps.rpc.RpcAddress
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.curator.framework.recipes.cache.{ChildData, PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode
import org.grapheco.commons.util.Logging
import org.grapheco.regionfs.GlobalSetting
import org.grapheco.regionfs.server.Region

import scala.collection.JavaConversions

/**
  * Created by bluejoe on 2020/2/26.
  */
object ZooKeeperClient extends Logging {
  def create(zks: String, connectionTimeout: Int = 3000, sessionTimeout: Int = 30000): ZooKeeperClient = {
    val retryPolicy = new ExponentialBackoffRetry(1000, 10);
    val curator =
      CuratorFrameworkFactory.builder()
        .connectString(zks)
        .connectionTimeoutMs(connectionTimeout)
        .sessionTimeoutMs(sessionTimeout)
        .retryPolicy(retryPolicy)
        .build();

    curator.start();

    //check root ZNode
    if (curator.checkExists().forPath("/regionfs") == null) {
      throw new RegionFsNotInitializedException();
    }

    new ZooKeeperClient(curator)
  }
}

class ZooKeeperClient(curator: CuratorFramework) {
  val pool = Executors.newFixedThreadPool(5);

  def createRegionWriteLock(regionId: Long): InterProcessSemaphoreMutex = {
    new InterProcessSemaphoreMutex(curator, s"/regionfs/region_write_lock_$regionId");
  }

  def createAbsentNodes() {
    curator.create().orSetData().forPath("/regionfs")
    curator.create().orSetData().forPath("/regionfs/nodes")
    curator.create().orSetData().forPath("/regionfs/regions")
  }

  def assertPathNotExists(path: String)(onAssertFailed: => Unit) = {
    if (curator.checkExists.forPath(path) != null) {
      onAssertFailed
      throw new ZNodeAlreadyExistExcetion(path);
    }
  }

  private def toByteArray(write: (DataOutputStream) => Unit): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)
    write(dos)
    baos.toByteArray
  }

  def updateRegionNode(nodeId: Int, region: Region) = {
    curator.setData().forPath(s"/regionfs/regions/${nodeId}_${region.regionId}", toByteArray(_.writeLong(region.revision)))
  }

  def createRegionNode(nodeId: Int, region: Region) = {
    curator.create().withMode(CreateMode.EPHEMERAL).forPath(
      s"/regionfs/regions/${nodeId}_${region.regionId}", toByteArray(_.writeLong(region.revision)))
  }

  def createNodeNode(nodeId: Int, address: RpcAddress) = {
    curator.create().withMode(CreateMode.EPHEMERAL).forPath(
      s"/regionfs/nodes/${nodeId}_${address.host}_${address.port}")
  }

  def close(): Unit = {
    curator.close()
    pool.shutdown()
  }

  def loadGlobalSetting(): GlobalSetting = {
    if (curator.checkExists().forPath("/regionfs/config") == null) {
      throw new GlobalSettingNotFoundException("/regionfs/config");
    }

    val bytes = curator.getData.forPath("/regionfs/config")
    val bais = new ByteArrayInputStream(bytes);
    val props = new Properties()
    props.load(bais)

    new GlobalSetting(props)
  }

  def saveglobalSetting(props: Properties): Unit = {
    val baos = new ByteArrayOutputStream();
    props.store(baos, "global setting of region-fs")
    val bytes = baos.toByteArray

    curator.create().orSetData().forPath("/regionfs/config", bytes)
  }

  def readNodeList(): Iterable[String] = {
    JavaConversions.iterableAsScalaIterable(curator.getChildren.forPath("/regionfs/nodes"))
  }

  def readRegionList(): Iterable[String] = {
    JavaConversions.iterableAsScalaIterable(curator.getChildren.forPath("/regionfs/regions"))
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
  def watchNodeList(handler: ParsedChildNodeEventHandler[(Int, RpcAddress)]): Closeable = {
    watchParsedChildrenPath("/regionfs/nodes", handler)((data: ChildData) => {
      val path = data.getPath.substring("/regionfs/nodes".length + 1)
      val splits = path.split("_")
      splits(0).toInt -> (RpcAddress(splits(1), splits(2).toInt))
    });
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
  def watchRegionList(handler: ParsedChildNodeEventHandler[(Long, Int, Long)]): Closeable = {
    watchParsedChildrenPath("/regionfs/regions", handler)((data: ChildData) => {
      val path = data.getPath.substring("/regionfs/regions".length + 1)
      val splits = path.split("_")
      val is = new DataInputStream(new ByteArrayInputStream(data.getData))
      val revision = is.readLong()
      is.close()
      (splits(1).toLong, splits(0).toInt, revision)
    });
  }

  private def watchParsedChildrenPath[T](parentPath: String, handler: ParsedChildNodeEventHandler[T])(parse: (ChildData) => T): Closeable = {
    watchChildrenPath(parentPath, new ChildNodeEventHandler() {
      override def onChildAdded(data: ChildData): Unit = {
        val t = parse(data)
        if (handler.accepts(t))
          handler.onCreated(t)
      }

      def onChildUpdated(data: ChildData): Unit = {
        val t = parse(data)
        if (handler.accepts(t))
          handler.onUpdated(t)
      }

      def onInitialized(batch: Iterable[ChildData]): Unit = {
        handler.onInitialized(batch.map(parse(_)).filter(handler.accepts(_)))
      }

      override def onChildRemoved(data: ChildData): Unit = {
        val t = parse(data)
        if (handler.accepts(t))
          handler.onCreated(t)
      }
    })
  }

  val currentClient = this;

  def watchChildrenPath(parentPath: String, handler: ChildNodeEventHandler): Closeable = {
    val childrenCache = new PathChildrenCache(curator, parentPath, true);
    childrenCache.getListenable().addListener(new PathChildrenCacheListener with Logging {
      override def childEvent(curatorFramework: CuratorFramework, pathChildrenCacheEvent: PathChildrenCacheEvent): Unit = {
        pathChildrenCacheEvent.getType match {
          case PathChildrenCacheEvent.Type.CHILD_ADDED =>
            handler.onChildAdded(pathChildrenCacheEvent.getData)

          case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
            handler.onChildRemoved(pathChildrenCacheEvent.getData)

          case PathChildrenCacheEvent.Type.CHILD_UPDATED =>
            handler.onChildUpdated(pathChildrenCacheEvent.getData)

          case _ =>
          //ignore!
        }
      }
    }, pool);

    //read initial cache
    childrenCache.start(StartMode.BUILD_INITIAL_CACHE);
    //parse initial cache
    handler.onInitialized(JavaConversions.iterableAsScalaIterable(childrenCache.getCurrentData))

    new Closeable {
      override def close(): Unit = {
        childrenCache.close()
      }
    }
  }
}

trait ChildNodeEventHandler {
  def onChildAdded(data: ChildData);

  def onChildUpdated(data: ChildData);

  def onInitialized(batch: Iterable[ChildData]);

  def onChildRemoved(data: ChildData);
}

trait ParsedChildNodeEventHandler[T] {
  def onCreated(t: T);

  def onUpdated(t: T);

  def onInitialized(batch: Iterable[T]);

  def accepts(t: T): Boolean;

  def onDeleted(t: T);
}


class RegionFsException(msg: String, cause: Throwable = null)
  extends RuntimeException(msg, cause) {

}

class RegionFsNotInitializedException extends
  RegionFsException(s"RegionFS cluster is not initialized") {

}

class InvalidZooKeeperConnectionStringException(zks: String) extends
  RegionFsException(s"RegionFS cluster is not initialized") {

}

class GlobalSettingNotFoundException(path: String) extends
  RegionFsException(s"zknode for global setting not exists: $path") {

}

class ZNodeAlreadyExistExcetion(path: String) extends
  RegionFsException(s"existing node found in zookeeper: ${path}") {

}