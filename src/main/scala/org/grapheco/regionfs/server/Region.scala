package org.grapheco.regionfs.server

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, Paths}
import java.util.concurrent.atomic.AtomicLong

import io.netty.buffer.{ByteBuf, Unpooled}
import net.neoremind.kraps.util.ByteBufferInputStream
import org.grapheco.commons.util.Logging
import org.grapheco.regionfs.client.RegionFsClientException
import org.grapheco.regionfs.util._
import org.grapheco.regionfs.{Constants, FileId, GlobalSetting}

import scala.collection.mutable.ArrayBuffer
import scala.collection.{JavaConversions, mutable}

/**
  * Created by bluejoe on 2019/8/30.
  */
case class RegionConfig(regionDir: File, globalSetting: GlobalSetting) {

}

class RegionOpLog(conf: RegionConfig){
  private val file = new File(conf.regionDir, "log")
  private val appender = new FileOutputStream(file, true).getChannel

  def logCreateFile(localId: Long): Long = {
    val buf = ByteBuffer.allocate(1024)
    buf.putLong(localId)
    buf.putLong(1)

    appender.write(buf)
  }

  def logRemoveFile(localId: Long): Long = {
    val buf = ByteBuffer.allocate(1024)
    buf.putLong(localId)
    buf.putLong(2)

    appender.write(buf)
  }
}

class RegionFileBuffer(conf: RegionConfig) {
  private val file = new File(conf.regionDir, "buffer")
  private val appender = new FileOutputStream(file, true).getChannel

  def append(localId: Long, buf: ByteBuffer, crc32: Long): Long = {
    val offset = appender.position()
    val buf1 = ByteBuffer.allocate(1024)
    buf1.putLong(localId)
    buf1.putLong(crc32)
    buf1.putLong(buf.remaining())
    appender.write(Array(buf1, buf))
    offset
  }

  def close(): Unit = {
    appender.close()
  }
}

case class MemFileEntry(localId: Long, buf: ByteBuffer, length: Long, crc32: Long)

class RegionMem(val conf: RegionConfig) {

  val mem = mutable.Map[Long, MemFileEntry]()

  def append(localId: Long, buf: ByteBuffer, crc32: Long): Unit = {
    mem += localId -> MemFileEntry(localId, buf.duplicate(), buf.remaining(), crc32)
  }

  def checkAndFlush(flush: (MemFileEntry) => Unit): Unit = {
    val totalSize = mem.values.reduce(_.length + _.length)
    if (totalSize > conf.globalSetting.maxRegionMemSize) {
      while (!mem.isEmpty) {
        val entry = mem.head
        flush(entry._2)
        mem.remove(entry._1)
      }
    }
  }
}

/**
  * metadata of a region
  */
case class FileMetadata(localId: Long, creationTime: Long, offset: Long, length: Long, crc32: Long, status: Byte) {
  def tail = offset + length
}

//[localId:Long][time:Long][offset:Long][length:Long][crc32:Long][status:Byte]
//TODO: use 2-level metadata to avoid unused localIds
//level1: [offset:Long]
//level2: [localId:Long][offset:Long][length:Long][crc32:Long][status:Byte]
class RegionMetadataStore(conf: RegionConfig) extends Logging {

  val file = new File(conf.regionDir, "meta")
  val fptr = new RandomAccessFile(file, "rw")

  if (fptr.length() == 0) {
    fptr.writeLong(Constants.CURRENT_VERSION.toLong)
    fptr.writeLong(System.currentTimeMillis())
  }
  else {
    val version = Version(fptr.readLong())
    if (version.compareTo(Constants.CURRENT_VERSION) > 0) {
      throw new HigherVersionRegionException(version)
    }

    if (version.compareTo(Constants.LOWEST_SUPPORTED_VERSION) < 0) {
      throw new RegionVersionIsTooLowException(version)
    }
  }

  val OFFSET_OF_CONTENT = 16

  val cache: Cache[Long, FileMetadata] = new FixSizedCache[Long, FileMetadata](1024)

  def entries(): Iterable[FileMetadata] = {
    (0 to count().toInt - 1).map(read(_).get)
  }

  val METADATA_BLOCK_FOR_READ = new Array[Byte](Constants.METADATA_ENTRY_LENGTH_WITH_PADDING)

  def createNextId(): Long = {
    fptr.synchronized {
      val localId = count()
      fptr.seek(localId * Constants.METADATA_ENTRY_LENGTH_WITH_PADDING)
      write(localId, Constants.FILE_STATUS_TO_WRITE, -1, -1, 0)
      localId
    }
  }

  def read(localId: Long): Option[FileMetadata] = {
    if (localId >= count()) {
      None
    }
    else {
      cache.get(localId).orElse {
        fptr.synchronized {
          fptr.seek(offsetOf(localId))
          fptr.readFully(METADATA_BLOCK_FOR_READ)
        }

        val dis = new DataInputStream(new ByteArrayInputStream(METADATA_BLOCK_FOR_READ))
        val info = FileMetadata(dis.readLong(), dis.readLong(), dis.readLong(), dis.readLong(), dis.readLong(), dis.readByte())
        dis.close()

        cache.put(localId, info)
        Some(info)
      }
    }
  }

  def write(localId: Long, status: Byte, offset: Long, length: Long, crc32: Long): Unit = {
    val block = new ByteArrayOutputStream()
    val dos = new DataOutputStream(block)
    dos.writeLong(localId)
    val time = System.currentTimeMillis()
    dos.writeLong(time)
    dos.writeLong(offset)
    dos.writeLong(length)
    dos.writeLong(crc32)
    dos.writeByte(status)
    //padding
    dos.write(new Array[Byte](Constants.METADATA_ENTRY_LENGTH_WITH_PADDING - dos.size()))

    fptr.synchronized {
      //maybe overwrite
      fptr.seek(offsetOf(localId))
      fptr.write(block.toByteArray)
    }

    dos.close()

    cache.put(localId, FileMetadata(localId, time, offset, length, crc32, status))
  }

  private def offsetOf(localId: Long): Long = {
    OFFSET_OF_CONTENT + Constants.METADATA_ENTRY_LENGTH_WITH_PADDING * localId
  }

  //since=0,tail=1
  def offerMetaPatch(sinceRevision: Long): Array[(Long, Long)] = {
    //since=10, tail=13
    var bodyPatchSize = 0L
    val tail = count()
    assert(sinceRevision < tail)

    var n = sinceRevision
    val ids = ArrayBuffer[(Long, Long)]()

    while (n < tail && bodyPatchSize < Constants.MAX_PATCH_SIZE) {
      val meta = read(n).get
      if (meta.status == Constants.FILE_STATUS_GLOBAL_WRITTEN || meta.status == Constants.FILE_STATUS_MERGED) {
        ids += (n -> meta.length)
        bodyPatchSize += meta.length
      }

      n += 1
    }

    //n=13
    ids.toArray
  }

  def close(): Unit = {
    fptr.close()
  }

  def count() = (fptr.length() - OFFSET_OF_CONTENT) / Constants.METADATA_ENTRY_LENGTH_WITH_PADDING
}

class RegionBodyStore(conf: RegionConfig) {
  //region file, one file for each region by far
  private val fileBody = new File(conf.regionDir, "body")
  lazy val fptr = new RandomAccessFile(fileBody, "rw")

  /**
    * @return (offset: Long, length: Long, actualWritten: Long)
    */
  def append(buf: ByteBuffer, crc: Long): (Long, Long, Long) = {
    val length = buf.remaining()
    //7-(x+7)%8
    val padding = new Array[Byte](Constants.REGION_FILE_ALIGNMENT_SIZE - 1 -
      (length + Constants.REGION_FILE_ALIGNMENT_SIZE - 1) % Constants.REGION_FILE_ALIGNMENT_SIZE)

    val (offset, written) =
      fptr.synchronized {
        val offset = fptr.length()
        fptr.seek(offset)
        fptr.getChannel.write(buf.duplicate())

        //write padding
        fptr.write(padding.map(_ => '*'.toByte))
        (offset, length + padding.length)
      }

    val rbuf = read(offset, length)
    val crc2 = CrcUtils.computeCrc32(rbuf)
    if (crc != crc2) {
      throw new WrittenMismatchedStreamException()
    }

    (offset, length, written)
  }

  def unwrite(offset: Long): Unit = {
    //truncate
    fptr.setLength(offset)
  }

  def offerBodyPatch(offset: Long, length: Long): ByteBuffer = {
    fptr.synchronized {
      fptr.getChannel.map(FileChannel.MapMode.READ_ONLY, offset, length)
    }
  }

  def close(): Unit = {
    fptr.close()
  }

  def read(offset: Long, length: Long): ByteBuffer = {
    assert(length >= 0)
    fptr.synchronized {
      fptr.getChannel.map(FileChannel.MapMode.READ_ONLY, offset, length)
    }
  }
}

/**
  * a Region store files in storeDir
  */
class Region(val nodeId: Int, val regionId: Long, val conf: RegionConfig, listener: RegionEventListener) extends Logging {
  val isPrimary = (regionId >> 16) == nodeId
  val isSecondary = !isPrimary

  private lazy val rlog = new RegionOpLog(conf)
  private lazy val rbuffer = new RegionFileBuffer(conf)
  private lazy val rmem = new RegionMem(conf)
  private lazy val rbody = new RegionBodyStore(conf)
  private lazy val rmeta = new RegionMetadataStore(conf)
  private lazy val rtrash = new RegionTrashStore(conf)

  def revision() = rmeta.count() //fids.current

  def fileCount() = rmeta.count() - rtrash.count()

  def bufferedFileCount() = rmem.mem.size

  def bodyLength() = rbody.fptr.length()

  //TODO: archive
  def isWritable = bodyLength <= conf.globalSetting.regionSizeLimit

  def listFiles(): Iterable[FileEntry] = {
    rmeta.entries().map { meta =>
      new FileEntry() {
        override val id = FileId.make(regionId, meta.localId)
        override val creationTime = meta.creationTime
        override val length = meta.length
        override val region = info
        override val status = meta.status

        override def processContent[T](f: (InputStream) => T) = {
          val is = new ByteBufferInputStream(read(meta.localId).get)
          val t = f(is)
          is.close()
          t
        }
      }
    }
  }

  def createLocalId(): Rollbackable = {
    val localId = rmeta.createNextId()
    traffic.toWrite(localId)
    Rollbackable.success(localId) {

    }
  }

  def markGlobalWritten(localId: Long, length: Long): Rollbackable = {
    rmeta.write(localId, Constants.FILE_STATUS_GLOBAL_WRITTEN, -1, length, 0)
    traffic.completeWrite(localId)
    Rollbackable.success(localId) {

    }
  }

  def writeLogFile(localId: Long, buf: ByteBuffer, crc32: Long): Rollbackable = {
    //save temp file
    /*
    rmeta.write(localId, Constants.FILE_STATUS_TO_WRITE, -1, -1, 0)
    rmem.write(localId, buf.duplicate(), crc32)
    */
    rbuffer.append(localId, buf, crc32)
    rmem.append(localId, buf, crc32)
    Rollbackable.success(localId) {

    }
  }

  def close(): Unit = {
    rbody.close()
    rmeta.close()
    rtrash.close()
  }

  def read(localId: Long): Option[ByteBuffer] = {
    if (rtrash.contains(localId))
      None
    else {
      rmeta.read(localId) match {
        case None => None
        case Some(meta) =>
          meta.status match {
            case Constants.FILE_STATUS_GLOBAL_WRITTEN =>
              Some(rmem.read(localId, meta.length))
            case Constants.FILE_STATUS_MERGED =>
              Some(rbody.read(meta.offset, meta.length))
            case _ => None
          }
      }
    }
  }

  def delete(localId: Long): Boolean = {
    if (rmeta.read(localId).isDefined && !rtrash.contains(localId)) {
      rtrash.append(localId)
      true
    }
    else {
      false
    }
  }

  def offerPatch(sinceRevision: Long): ByteBuf = {
    val buf = Unpooled.buffer()

    val rev = this.synchronized {
      revision
    }

    //1!=0
    if (rev == sinceRevision) {
      buf.writeByte(Constants.MARK_GET_REGION_PATCH_ALREADY_UP_TO_DATE).writeLong(regionId)
      buf
    }
    else {
      val metas = rmeta.offerMetaPatch(sinceRevision)

      //flag, regionId, revision, fileCount
      buf.writeByte(Constants.MARK_GET_REGION_PATCH_OK).
        writeLong(regionId).writeLong(rev).writeInt(metas.length)

      metas.foreach { kv =>
        this.read(kv._1).map {
          bodybuf =>
            buf.writeLong(kv._1).writeLong(kv._2).writeLong(CrcUtils.computeCrc32(bodybuf.duplicate()))
            buf.writeBytes(bodybuf)
        }
      }

      buf
    }
  }

  //return value means if update is ok
  def applyPatch(is: InputStream, callbackOnUpdated: => Unit): Boolean = {
    val dis = new DataInputStream(is)
    val mark = dis.readByte()
    mark match {
      case Constants.MARK_GET_REGION_PATCH_ALREADY_UP_TO_DATE =>
        false

      case Constants.MARK_GET_REGION_PATCH_SERVER_IS_BUSY =>
        val regionId = dis.readLong()
        if (logger.isDebugEnabled)
          logger.debug(s"server is busy now: ${regionId >> 16}")

        false
      case Constants.MARK_GET_REGION_PATCH_OK =>
        val (regionId, revision, patchFileCount) = (
          dis.readLong(), dis.readLong(), dis.readInt())

        for (i <- 1 to patchFileCount) {
          val (localId, fileSize, crc32) = (dis.readLong(), dis.readLong(), dis.readLong())
          val buf = Unpooled.buffer(1024)
          buf.writeBytes(is, fileSize.toInt)

          val tx = Atomic("save local file") {
            case _ =>
              this.writeLogFile(localId, buf.nioBuffer(), crc32)
          } --> Atomic("mark global written") {
            case _ =>
              this.markGlobalWritten(localId, fileSize)
          }

          TransactionRunner.perform(tx, regionId, RetryStrategy.FOR_TIMES(conf.globalSetting.maxWriteRetryTimes))
        }

        true
    }
  }

  def cleanup(stopFlag: () => Boolean): Iterator[Long] = {
    val list: Iterator[Long] = rmem.list()
    for (localId <- list if !stopFlag()) {
      rmeta.read(localId).map { meta =>
        meta.status match {
          case Constants.FILE_STATUS_GLOBAL_WRITTEN =>
            //save in body
            val buf = rmem.read(localId, meta.length)
            val tx = Atomic("append to body") {
              case _ =>
                val crc32 = CrcUtils.computeCrc32(buf.duplicate())
                val (offset: Long, length: Long, actualWritten: Long) = rbody.append(buf, crc32)
                Rollbackable.success((offset, length, actualWritten, crc32)) {
                  rbody.unwrite(offset)
                }
            } --> Atomic("marked merged") {
              case (offset: Long, length: Long, actualWritten: Long, crc32: Long) =>
                rmeta.write(localId, Constants.FILE_STATUS_MERGED, offset, length, crc32)
                Rollbackable.success(true) {

                }
            } --> Atomic("delete buffered file") {
              case _ =>
                rmem.delete(localId)
                Rollbackable.success(true) {
                }
            }

            TransactionRunner.perform(tx, regionId, RetryStrategy.FOR_TIMES(conf.globalSetting.maxWriteRetryTimes))

            if (logger.isTraceEnabled()) {
              logger.trace(s"[region-$regionId@$nodeId] local file merged: $localId")
            }
          case _ =>
            if (traffic.isToWrite(localId)) {
              if (logger.isTraceEnabled()) {
                logger.trace(s"[region-$regionId@$nodeId] cleaned local file: $localId")
              }

              rmem.delete(localId)
            }
        }
      }
    }

    list
  }

  def info = RegionInfo(nodeId, regionId, revision, isPrimary, isWritable, bodyLength)
}

/**
  * RegionManager manages local regions stored in storeDir
  */
class LocalRegionManager(nodeId: Int, storeDir: File, globalSetting: GlobalSetting, listener: RegionEventListener) extends Logging {
  val regions = mutable.Map[Long, Region]()
  val regionIdSerial = new AtomicLong(0)
  val ring = new Ring[Long]()

  /*
   layout of storeDir
    ./1
      body
      meta
    ./2
      body
      meta
    ...
  */
  regions ++= storeDir.listFiles().
    filter { file =>
      !file.isHidden && file.isDirectory
    }.
    map { file =>
      val id = file.getName.toLong
      id -> new Region(nodeId, id, RegionConfig(file, globalSetting), listener)
    }

  ring ++= regions.keys

  if (logger.isInfoEnabled())
    logger.info(s"[node-$nodeId] loaded local regions: ${regions.keySet}")

  regionIdSerial.set((List(0L) ++ regions.map(_._1 >> 16).toList).max)

  def createNew() = {
    _createNewRegion((nodeId << 16) + regionIdSerial.incrementAndGet())
  }

  def createSecondaryRegion(regionId: Long) = {
    _createNewRegion(regionId)
  }

  def get(id: Long): Option[Region] = regions.get(id)

  def update(region: Region): Region = {
    val newRegion = new Region(nodeId, region.regionId, region.conf, listener)

    this.synchronized {
      regions(region.regionId) = newRegion
      //TODO: should close this object
      //region.close()
    }

    newRegion
  }

  private def _createNewRegion(regionId: Long) = {
    //create files
    val region = {
      //create a new region
      val regionDir = new File(storeDir, s"$regionId")
      regionDir.mkdir()

      new File(regionDir, "body").createNewFile()
      new File(regionDir, "meta").createNewFile()
      new File(regionDir, "trash").createNewFile()

      if (logger.isTraceEnabled())
        logger.trace(s"[region-$regionId@$nodeId] created: dir=$regionDir")

      new Region(nodeId, regionId, RegionConfig(regionDir, globalSetting), listener)
    }

    listener.handleRegionEvent(CreateRegionEvent(region))
    regions.synchronized {
      regions += (regionId -> region)
      ring += regionId
    }
    region
  }
}

class WrittenMismatchedStreamException extends RegionFsServerException("mismatched checksum exception on write time") {

}

class ServerIsBusyException(nodeId: Int) extends RegionFsClientException(s"server is busy: $nodeId") {

}

trait RegionEvent {

}

case class CreateRegionEvent(region: Region) extends RegionEvent {

}

case class WriteRegionEvent(region: Region) extends RegionEvent {

}

//disable write
case class ArchiveRegionEvent(region: Region) extends RegionEvent {

}

//reorganize
case class RefactorRegionEvent(region: Region) extends RegionEvent {

}

trait RegionEventListener {
  def handleRegionEvent(event: RegionEvent)
}

class WrongRegionPatchSizeException() extends RegionFsServerException(s"wrong region patch") {

}

class WrongRegionPatchStreamException extends RegionFsServerException(s"wrong region patch") {

}

case class RegionInfo(nodeId: Int, regionId: Long, revision: Long, isPrimary: Boolean, isWritable: Boolean, length: Long) {

}

class HigherVersionRegionException(version: Version) extends RegionFsServerException(
  s"higher version of region: ${version.toString} > ${Constants.CURRENT_VERSION.toString}") {

}

class RegionVersionIsTooLowException(version: Version) extends RegionFsServerException(
  s"version of region is too low: ${version.toString} < ${Constants.LOWEST_SUPPORTED_VERSION.toString}") {

}

trait FileEntry {
  val id: FileId
  val region: RegionInfo
  val length: Long
  val creationTime: Long
  val status: Byte

  def processContent[T](f: (InputStream) => T): T;
}