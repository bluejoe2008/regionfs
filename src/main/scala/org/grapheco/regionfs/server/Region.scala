package org.grapheco.regionfs.server

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.concurrent.atomic.AtomicLong

import io.netty.buffer.{ByteBuf, Unpooled}
import net.neoremind.kraps.util.ByteBufferInputStream
import org.grapheco.commons.util.Logging
import org.grapheco.regionfs.client.RegionFsClientException
import org.grapheco.regionfs.util._
import org.grapheco.regionfs.{Constants, FileId, GlobalSetting}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by bluejoe on 2019/8/30.
  */
case class RegionConfig(regionDir: File, globalSetting: GlobalSetting) {

}

class RegionLocalIdGen(conf: RegionConfig) {
  private val file = new File(conf.regionDir, "idgen")
  val ID_SEQUENCE_SIZE = 10
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

  private def loadNextSequence(): Unit = {
    val current = id.get()
    val end = current + ID_SEQUENCE_SIZE - 1
    val fos = new DataOutputStream(new FileOutputStream(file))
    fos.writeLong(end)
    fos.close()
    bound.set(end)
  }

  def nextId(): Long = {
    val nid = id.incrementAndGet()
    //all ids consumed
    if (nid > bound.get()) {
      loadNextSequence()
    }
    nid
  }

  def flush(): Unit = {
    val fos = new DataOutputStream(new FileOutputStream(file))
    fos.writeLong(id.get())
    fos.close()
  }

  def close(): Unit = {
    flush()
  }
}

class RegionWal(conf: RegionConfig) {
  private val file = new File(conf.regionDir, "wal")
  private val appender = new FileOutputStream(file, true).getChannel

  def logCreateFile(localId: Long): Long = {
    val buf = ByteBuffer.allocate(1024)
    buf.putLong(localId)
    buf.putLong(1)
    buf.flip()
    appender.write(buf)
  }

  def logRemoveFile(localId: Long): Long = {
    val buf = ByteBuffer.allocate(1024)
    buf.putLong(localId)
    buf.putLong(2)

    appender.write(buf)
  }

  def close(): Unit = {
    appender.close()
  }
}

class RegionFileStage(conf: RegionConfig) {
  private val file = new File(conf.regionDir, "stage")
  private val appender = new FileOutputStream(file, true).getChannel

  def append(localId: Long, buf: ByteBuffer, crc32: Long): Long = {
    val offset = appender.position()
    val buf1 = ByteBuffer.allocate(1024)
    buf1.putLong(localId)
    buf1.putLong(crc32)
    buf1.putLong(buf.remaining())
    buf1.flip()
    appender.write(Array(buf1, buf))
    offset
  }

  def close(): Unit = {
    appender.close()
  }
}

case class MemFileEntry(localId: Long, buf: ByteBuf, length: Long, crc32: Long)

class RegionMem(val conf: RegionConfig, write: (MemFileEntry) => Unit, delete: (Long) => Unit)
  extends Logging {

  private val staged = mutable.Map[Long, MemFileEntry]()
  private val committed = mutable.Map[Long, MemFileEntry]()
  private val removed = mutable.Set[Long]()

  def stage(localId: Long, buf: ByteBuffer, crc32: Long): Unit = {
    val buf2 = Unpooled.copiedBuffer(buf)
    staged += localId -> MemFileEntry(localId, buf2, buf2.readableBytes(), crc32)
  }

  def commit(localId: Long): Unit = {
    committed += localId -> (staged.remove(localId).get)
  }

  def remove(localId: Long): Unit = {
    removed += localId
  }

  def read(localId: Long): (Option[ByteBuf], Boolean) = {
    committed.get(localId).map(_.buf) -> removed.contains(localId)
  }

  def checkIfFull(): Unit = {
    val totalSize = (committed -- removed).values.map(_.length).reduce(_ + _)
    totalSize > conf.globalSetting.maxRegionMemSize
  }

  def flush(): Unit = {
    val consumedCreated = ArrayBuffer[Long]()

    committed.foreach { kv =>
      if (!removed.contains(kv._1)) {
        write(kv._2)
      }
      consumedCreated += kv._1
    }

    val consumeRemoved = ArrayBuffer[Long]()
    committed.foreach { kv =>
      if (!committed.contains(kv._1)) {
        delete(kv._1)
      }
      consumeRemoved += kv._1
    }

    this.synchronized {
      committed --= consumedCreated
      removed --= consumeRemoved
    }
  }

  def close(): Unit = {
    flush()
  }

  def countCommitted = committed.size - removed.size
}

/**
  * metadata of a region
  */
case class FileMetadata(localId: Long, creationTime: Long, offset: Long, length: Long, crc32: Long, status: Byte) {
  def tail = offset + length
}

//use 2-level metadata to avoid unused localIds
//level1: [offset:Long], offset=0 means unused
//level2: [localId:Long][offset:Long][length:Long][crc32:Long][status:Byte]
class RegionMetadataStore(conf: RegionConfig) extends Logging {

  val file1 = new File(conf.regionDir, "meta1")
  val fptr1 = new RandomAccessFile(file1, "rw")

  val file2 = new File(conf.regionDir, "meta2")
  val fptr2 = new RandomAccessFile(file2, "rw")

  //write version number
  if (fptr2.length() == 0) {
    fptr2.writeLong(Constants.CURRENT_VERSION.toLong)
    fptr2.writeLong(System.currentTimeMillis())
  }
  else {
    val version = Version(fptr2.readLong())
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

  def read(localId: Long): Option[FileMetadata] = {
    if (localId >= count()) {
      None
    }
    else {
      cache.get(localId).orElse {
        val offset = fptr1.synchronized {
          fptr1.seek(localId * 8)
          fptr1.readLong()
        }

        fptr2.synchronized {
          fptr2.seek(offset)
          fptr2.readFully(METADATA_BLOCK_FOR_READ)
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

    val offset2 = fptr2.length()

    fptr2.synchronized {
      //maybe overwrite
      fptr2.seek(offset2)
      fptr2.write(block.toByteArray)
    }

    fptr1.synchronized {
      fptr1.seek(localId * 8)
      fptr1.writeLong(offset2)
    }

    dos.close()

    cache.put(localId, FileMetadata(localId, time, offset, length, crc32, status))
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
      if (meta.status == Constants.FILE_STATUS_MERGED) {
        ids += (n -> meta.length)
        bodyPatchSize += meta.length
      }

      n += 1
    }

    //n=13
    ids.toArray
  }

  def close(): Unit = {
    fptr1.close()
    fptr2.close()
  }

  def count() = (fptr2.length() - OFFSET_OF_CONTENT) / Constants.METADATA_ENTRY_LENGTH_WITH_PADDING
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
  * new file-->stage-->wal-->body
  */
class Region(val nodeId: Int, val regionId: Long, val conf: RegionConfig, listener: RegionEventListener) extends Logging {
  val isPrimary = (regionId >> 16) == nodeId
  val isSecondary = !isPrimary

  private lazy val wal = new RegionWal(conf)
  private lazy val idgen = new RegionLocalIdGen(conf)
  private lazy val stage = new RegionFileStage(conf)
  private lazy val mem = new RegionMem(conf, performWrite(_), performDelete(_))
  private lazy val body = new RegionBodyStore(conf)
  private lazy val meta = new RegionMetadataStore(conf)

  def revision() = meta.count() //fids.current

  def fileCount() = meta.count() + mem.countCommitted

  def unmergedFileCount() = mem.countCommitted

  def bodyLength() = body.fptr.length()

  //TODO: archive
  def isWritable = bodyLength <= conf.globalSetting.regionSizeLimit

  def listFiles(): Iterable[FileEntry] = {
    //FIXME
    meta.entries().map { meta =>
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
    val localId = idgen.nextId()
    Rollbackable.success(localId) {
    }
  }

  def commitFile(localId: Long): Rollbackable = {
    wal.logCreateFile(localId)
    mem.commit(localId)
    Rollbackable.success(localId) {

    }
  }

  def stageFile(localId: Long, buf: ByteBuffer, crc32: Long): Rollbackable = {
    stage.append(localId, buf.duplicate(), crc32)
    mem.stage(localId, buf.duplicate(), crc32)
    Rollbackable.success(localId) {

    }
  }

  def close(): Unit = {
    wal.close()
    idgen.close()
    mem.close()
    body.close()
    meta.close()
  }

  def read(localId: Long): Option[ByteBuffer] = {
    mem.read(localId) match {
      case (_, true) => None
      case (Some(buf), false) => Some(buf.nioBuffer())
      case (None, false) =>
        meta.read(localId) match {
          case None =>
            None
          case Some(meta) =>
            Some(body.read(meta.offset, meta.length))
        }
    }
  }

  def delete(localId: Long): Boolean = {
    wal.logRemoveFile(localId)
    mem.remove(localId)
    true
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
      val metas = meta.offerMetaPatch(sinceRevision)

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

          val tx = Atomic("file->stage") {
            case _ =>
              this.stageFile(localId, buf.nioBuffer(), crc32)
          } --> Atomic("file->commit") {
            case _ =>
              this.commitFile(localId)
          }

          TransactionRunner.perform(tx, regionId, RetryStrategy.FOR_TIMES(conf.globalSetting.maxWriteRetryTimes))
        }

        true
    }
  }

  def performDelete(id: Long): Unit = {

  }

  def performWrite(file: MemFileEntry): Unit = {
    //write body
    val localId = file.localId
    val crc32 = file.crc32
    val (offset: Long, length: Long, actualWritten: Long) = body.append(file.buf.nioBuffer(), crc32)
    //write metadata
    meta.write(localId, Constants.FILE_STATUS_MERGED, offset, length, crc32)

    if (logger.isDebugEnabled)
      logger.debug(s"[region-${regionId}@$nodeId] written : $localId")
  }

  def flush() = {
    mem.flush()
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
      new File(regionDir, "meta1").createNewFile()
      new File(regionDir, "meta2").createNewFile()
      new File(regionDir, "wal").createNewFile()
      new File(regionDir, "stage").createNewFile()

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