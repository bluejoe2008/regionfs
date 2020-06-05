package org.grapheco.regionfs.util

import java.io._
import java.util.concurrent.atomic.AtomicLong

import org.grapheco.commons.util.Logging
import org.grapheco.regionfs.server.RegionFsServerException

/**
  * Created by bluejoe on 2020/6/4.
  * NOTE: id starts from 1
  */
class FileBasedIdGen(file: File, sequenceSize: Int) extends Logging {

  private val id: AtomicLong = {
    if (file.length() == 0) {
      new AtomicLong(0)
    }
    else {
      val fis = new DataInputStream(new FileInputStream(file))
      val current = fis.readLong()
      fis.close()

      new AtomicLong(current)
    }
  }

  private val bound = new AtomicLong(0)

  private def slideDown(current: Long): Unit = {
    val end = current + sequenceSize - 1
    writeId(end)
    bound.set(end)
  }

  private def slideDownIfNeeded(nid: Long): Unit = {
    if (nid > bound.get()) {
      slideDown(nid)
    }
  }

  def currentId() = id.get()

  def update(newId: Long): Unit = {
    if (newId < currentId()) {
      throw new LowerIdSetException(newId)
    }

    id.set(newId)
    slideDownIfNeeded(newId)
  }

  def nextId(): Long = {
    val nid = id.incrementAndGet()
    //all ids consumed
    slideDownIfNeeded(nid)
    nid
  }

  //save current id
  def flush(): Unit = {
    writeId(id.get())
  }

  private def writeId(num: Long): Unit = {
    val fos = new DataOutputStream(new FileOutputStream(file))
    fos.writeLong(num)
    fos.close()

    if (logger.isTraceEnabled)
      logger.trace(s"current: ${id.get()}, written: $num")
  }
}

class LowerIdSetException(id: Long) extends RegionFsServerException(s"lower id set: $id") {

}