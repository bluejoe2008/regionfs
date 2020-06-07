package org.grapheco.regionfs.server

import java.util.concurrent.atomic.AtomicBoolean

import io.netty.buffer.Unpooled
import org.grapheco.commons.util.Logging
import org.grapheco.hippo.util.ByteBufferInputStream
import org.grapheco.regionfs.client.FsNodeClient
import org.grapheco.regionfs.util.{CompositeParsedChildNodeEventHandler, NodeServerInfo, ParsedChildNodeEventHandler}
import org.grapheco.regionfs.{Constants, GlobalSetting}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Created by bluejoe on 2020/3/20.
  */
//report local secondary regions
//stores remote secondary regions
class RemoteRegionWatcher(nodeId: Int, globalSetting: GlobalSetting,
                          zkNodeEventHandlers: CompositeParsedChildNodeEventHandler[NodeServerInfo],
                          localRegionManager: LocalRegionManager,
                          clientOf: (Int) => FsNodeClient) {
  private val _mapRemoteSecondaryRegions = mutable.Map[Long, mutable.Map[Int, RegionInfo]]()

  def cachedRemoteSecondaryRegions(regionId: Long) = _mapRemoteSecondaryRegions(regionId).values

  def cacheRemoteSeconaryRegions(regions: Array[RegionInfo]): Unit = {
    regions.
      filter(info => localRegionManager.get(info.regionId).isDefined).
      groupBy(_.regionId).
      foreach(x =>
        _mapRemoteSecondaryRegions.getOrElseUpdate(
          x._1, mutable.Map[Int, RegionInfo]()) ++= x._2.map(t => t.nodeId -> t))
  }

  //watch zknodes
  zkNodeEventHandlers.addHandler(new ParsedChildNodeEventHandler[NodeServerInfo] {
    override def onCreated(t: NodeServerInfo): Unit = {
      reportLocalSeconaryRegions(t.nodeId)
    }

    override def onUpdated(t: NodeServerInfo): Unit = {

    }

    override def onInitialized(batch: Iterable[NodeServerInfo]): Unit = {
      batch.foreach(t => reportLocalSeconaryRegions(t.nodeId))
    }

    override def onDeleted(t: NodeServerInfo): Unit = {
      _mapRemoteSecondaryRegions.foreach {
        _._2 -= t.nodeId
      }
    }

    override def accepts(t: NodeServerInfo): Boolean = t.nodeId != nodeId
  })

  private def reportLocalSeconaryRegions(nodeId: Int): Unit = {
    val localRegions = localRegionManager.regions.values.filter(region => region.nodeId == nodeId && region.isSecondary)
    //noinspection EmptyCheck
    if (localRegions.nonEmpty) {
      val client = clientOf(nodeId)
      client.registerSeconaryRegions(localRegions.map(_.info).toArray)
    }
  }

  private val _backgroundWorkers = ArrayBuffer[() => Unit]()

  {
    //primary region follower
    if (globalSetting.consistencyStrategy == Constants.CONSISTENCY_STRATEGY_EVENTUAL) {
      val watcher = new PrimaryRegionWatcher(globalSetting, nodeId, localRegionManager, clientOf(_)).start()
      _backgroundWorkers += (() => {
        watcher.stop()
      })
    }

    //region file clean up
    val cleaner = new LocalRegionCleaner(globalSetting, nodeId, localRegionManager).start()
    _backgroundWorkers += (() => {
      cleaner.stop()
    })
  }

  def getSecondaryRegions(regionId: Long): Array[RegionInfo] = {
    _mapRemoteSecondaryRegions.get(regionId).map(_.values.toArray).getOrElse(Array())
  }

  def close(): Unit = {
    _backgroundWorkers.foreach(_.apply())
  }
}

trait InterruptableWorker {
  val sleepInterval: Long

  def runOnce(stopFlag: => Boolean): Unit

  def onStarted(): Unit

  def beforeTerminate(): Unit

  private var running: Boolean = false
  private var sleeping: Boolean = false
  private val stopFlag = new AtomicBoolean(false)
  private val thread: Thread = new Thread(new Runnable() {

    override def run(): Unit = {
      running = true
      while (!stopFlag.get()) {
        sleeping = true
        try {
          Thread.sleep(sleepInterval)
        }
        catch {
          case e: InterruptedException =>
            stopFlag.set(true)
        }

        sleeping = false
        if (!stopFlag.get()) {
          runOnce {
            stopFlag.get()
          }
        }
      }

      running = false
    }
  })

  final def start(): this.type = {
    thread.start()
    onStarted()

    this
  }

  final def stop(): Unit = {
    if (running) {
      stopFlag.set(true)
      beforeTerminate()

      if (sleeping) {
        thread.interrupt()
      }

      thread.join()
    }
  }
}

//flush region files
class LocalRegionCleaner(conf: GlobalSetting,
                         nodeId: Int,
                         localRegionManager: LocalRegionManager)
  extends InterruptableWorker with Logging {
  val sleepInterval: Long = conf.regionFileCleanupInterval

  def runOnce(stopFlag: => Boolean): Unit = {
    val regions = localRegionManager.regions.values
    for (region <- regions if !stopFlag) {
      try {
        region.flushAll(false)
      }
      catch {
        case t: Throwable =>
          if (logger.isWarnEnabled())
            logger.warn(t.getMessage)
      }
    }
  }

  def onStarted(): Unit = {
    if (logger.isTraceEnabled()) {
      logger.trace(s"[node-$nodeId] started ${this.getClass.getSimpleName}...")
    }
  }

  def beforeTerminate(): Unit = {
    if (logger.isTraceEnabled()) {
      logger.trace(s"[node-$nodeId] stopping ${this.getClass.getSimpleName}...")
    }
  }
}

//sync files from primary region
class PrimaryRegionWatcher(conf: GlobalSetting,
                           nodeId: Int,
                           localRegionManager: LocalRegionManager,
                           clientOf: (Int) => FsNodeClient)
  extends InterruptableWorker with Logging {

  val sleepInterval: Long = conf.regionVersionCheckInterval

  def runOnce(stopFlag: => Boolean): Unit = {
    val secondaryRegions = localRegionManager.regions.values.filter(!_.isPrimary).groupBy(x =>
      (x.regionId >> 16).toInt)
    for (x <- secondaryRegions if !stopFlag) {
      try {
        val regionIds = x._2.map(_.regionId).toArray
        val primaryNodeId = x._1
        val infos = Await.result(clientOf(primaryNodeId).getRegionInfos(regionIds), Duration("2s"))
        infos.foreach(status => {
          val localRegion = localRegionManager.regions(status.regionId)
          //local region is old
          val targetRevision: Long = status.revision
          val localRevision: Long = localRegion.revision
          if (targetRevision > localRevision) {
            if (logger.isTraceEnabled())
              logger.trace(s"[region-${localRegion.regionId}@$nodeId] found new version : $targetRevision@$primaryNodeId>$localRevision@$nodeId")

            Await.result(clientOf(primaryNodeId).getPatch(
              localRegion.regionId, localRevision, (buf) => {
                localRegion.applyPatch(Unpooled.wrappedBuffer(buf), {
                  val updatedRegion = localRegionManager.update(localRegion)
                  if (logger.isTraceEnabled())
                    logger.trace(s"[region-${localRegion.regionId}@$nodeId] updated: $localRevision->${updatedRegion.revision}")
                })
              }), Duration("10s"))
          }
        })
      }
      catch {
        case t: Throwable =>
          if (logger.isWarnEnabled())
            logger.warn(t.getMessage)
      }
    }
  }

  def onStarted(): Unit = {
    if (logger.isTraceEnabled()) {
      logger.trace(s"[node-$nodeId] started ${this.getClass.getSimpleName}...")
    }
  }

  def beforeTerminate(): Unit = {
    if (logger.isTraceEnabled()) {
      logger.trace(s"[node-$nodeId] stoping ${this.getClass.getSimpleName}...")
    }
  }
}