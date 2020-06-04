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

  def currentId() = id.get()

  def nextId(): Long = {
    val nid = id.incrementAndGet()
    //all ids consumed
    if (nid > bound.get()) {
      loadNextSequence()
    }
    nid
  }

  //save current id
  def flush(): Unit = {
    val fos = new DataOutputStream(new FileOutputStream(file))
    fos.writeLong(id.get())
    fos.close()
  }

  def close(): Unit = {
    flush()
  }
}

class RegionOperationLog(conf: RegionConfig) extends FileBasedWAL {

  protected def makeFile(postfix: String): File = new File(conf.regionDir, s"log${postfix}")

  def logCreateFile(localId: Long): Unit = {
    val buf = ByteBuffer.allocate(1024)
    buf.putLong(localId)
    buf.putLong(1)
    buf.flip()
    append(_.write(buf))
  }

  def logRemoveFile(localId: Long): Unit = {
    val buf = ByteBuffer.allocate(1024)
    buf.putLong(localId)
    buf.putLong(2)
    append(_.write(buf))
  }
}

class RegionFileStage(conf: RegionConfig) extends FileBasedWAL {
  protected def makeFile(postfix: String): File = new File(conf.regionDir, s"stage${postfix}")

  def append(localId: Long, buf: ByteBuffer, crc32: Long): Unit = {
    val buf1 = ByteBuffer.allocate(1024)
    buf1.putLong(localId)
    buf1.putLong(crc32)
    buf1.putLong(buf.remaining())
    buf1.flip()

    append(_.write(Array(buf1, buf)))
  }
}

case class MemFileEntry(localId: Long, buf: ByteBuf, length: Long, crc32: Long)

class RegionMem(regionName: RegionName, conf: RegionConfig, write: (Iterable[MemFileEntry]) => Unit, delete: (Iterable[Long]) => Unit)
  extends Logging with Forkable {
  private val staged = mutable.Map[Long, MemFileEntry]()
  private val committed = mutable.Map[Long, MemFileEntry]()
  private val removed = mutable.Set[Long]()
  private var _lastFlushTime: Long = 0

  def stage(localId: Long, buf: ByteBuffer, crc32: Long): Unit = {
    val buf2 = Unpooled.copiedBuffer(buf)
    staged += localId -> MemFileEntry(localId, buf2, buf2.readableBytes(), crc32)
  }

  def commit(localId: Long): MemFileEntry = {
    val entry = staged.remove(localId).get
    committed += localId -> entry
    entry
  }

  def remove(localId: Long): Boolean = {
    if (removed.contains(localId)) {
      //delete twice?
      false
    }
    else {
      removed += localId
      true
    }
  }

  def read(localId: Long): (Option[MemFileEntry], Boolean) = {
    committed.get(localId) -> removed.contains(localId)
  }

  def isDirtyEnough(): Boolean = {
    if (!committed.isEmpty) {
      val time = System.currentTimeMillis()
      if (time - _lastFlushTime > conf.globalSetting.maxRegionMemDirtyTime)
        true
      else
        (committed.size + removed.size) > conf.globalSetting.maxRegionMemEntryCount ||
          committed.map(_._2.length).sum > conf.globalSetting.maxRegionMemSize
    }
    else {
      false
    }
  }

  def fork(): Fork = {
    _lastFlushTime = System.currentTimeMillis()

    val committed2 = mutable.Map[Long, MemFileEntry]()
    val removed2 = mutable.Set[Long]()

    this.synchronized {
      committed2 ++= committed
      removed2 ++= removed
    }

    new Fork {
      override def success(): Unit = {
        val toWrite = ArrayBuffer[MemFileEntry]()
        val toDelete = ArrayBuffer[Long]()
        committed2.foreach { kv =>
          if (!removed2.contains(kv._1)) {
            toWrite += kv._2
          }
        }

        committed2.foreach { kv =>
          if (!committed.contains(kv._1)) {
            toDelete += kv._1
          }
        }

        write(toWrite)
        delete(toDelete)

        this.synchronized {
          committed --= toWrite.map(_.localId)
          removed --= toDelete
        }

        if (logger.isDebugEnabled)
          logger.debug(s"${regionName} flushed: ${toWrite.map(_.localId).toList}")
      }
    }
  }

  def close(): Unit = {

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
    (1 to count().toInt).map(read(_).get)
  }

  val METADATA_BLOCK_FOR_READ = new Array[Byte](Constants.METADATA_ENTRY_LENGTH_WITH_PADDING)

  //localId starts with 1
  def read(localId: Long): Option[FileMetadata] = {
    if (localId > count()) {
      None
    }
    else {
      cache.get(localId).orElse {
        val offset = fptr1.synchronized {
          fptr1.seek((localId - 1) * 8)
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
    dos.close()

    //localId starts with 1
    fptr1.synchronized {
      fptr1.seek((localId - 1) * 8)
      fptr1.writeLong(offset2)
    }

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

  def count() = fptr1.length() / 8
}

class RegionBodyStore(conf: RegionConfig) {
  //region file, one file for each region by far
  private val file = new File(conf.regionDir, "body")
  private lazy val fptr = new RandomAccessFile(file, "rw")

  def bodySize() = fptr.length()

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

case class RegionName(nodeId: Int, regionId: Long) {
  override def toString = s"[region-${regionId}@$nodeId]"
}

/**
  * a Region store files in storeDir
  * new file-->stage-->wal-->body
  */
class Region(val nodeId: Int, val regionId: Long, val conf: RegionConfig, listener: RegionEventListener) extends Logging {
  val isPrimary = (regionId >> 16) == nodeId
  val isSecondary = !isPrimary

  val regionName = RegionName(nodeId, regionId)

  private lazy val oplog = new RegionOperationLog(conf)
  private lazy val idgen = new RegionLocalIdGen(conf)
  private lazy val stage = new RegionFileStage(conf)
  private lazy val mem = new RegionMem(regionName, conf, performWrite, performDelete)
  private lazy val body = new RegionBodyStore(conf)
  private lazy val meta = new RegionMetadataStore(conf)

  def revision() = idgen.currentId

  private val _regionFileCount = new AtomicLong(meta.count())
  private val _regionSize = new AtomicLong(body.bodySize)

  def fileCount(): Long = _regionFileCount.get()

  def unmergedFileCount(): Long = mem.countCommitted

  def regionSize(): Long = _regionSize.get

  private def fork(): Fork = {
    val forks = this.synchronized {
      Array(oplog.fork(), stage.fork(), mem.fork())
    }

    new Fork {
      def success() {
        forks.reverse.foreach(_.success())
      }
    }
  }

  //TODO: archive
  def isWritable = regionSize <= conf.globalSetting.regionSizeLimit

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
    oplog.logCreateFile(localId)
    val file = mem.commit(localId)

    _regionFileCount.incrementAndGet()
    _regionSize.addAndGet(file.length)

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
    flushAll(true)
    mem.close()
    oplog.close()
    idgen.close()
    body.close()
    meta.close()
  }

  def read(localId: Long): Option[ByteBuffer] = {
    mem.read(localId) match {
      case (_, true) => None
      case (Some(file), false) => Some(file.buf.nioBuffer())
      case (None, false) =>
        meta.read(localId) match {
          case None =>
            None
          case Some(meta) =>
            Some(body.read(meta.offset, meta.length))
        }
    }
  }

  def delete(fileId: FileId): Unit = {
    val localId = fileId.localId
    if (revision < localId)
      throw new FileNotFoundException(nodeId, fileId)

    read(localId).map { buf =>
      if (mem.remove(localId)) {
        oplog.logRemoveFile(localId)

        _regionFileCount.decrementAndGet()
        _regionSize.addAndGet(-1 * buf.remaining())
      }
    }.orElse {
      throw new FileNotFoundException(nodeId, fileId)
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

  def performDelete(ids: Iterable[Long]): Unit = {

  }

  def performWrite(files: Iterable[MemFileEntry]): Unit = {
    val ids = files.map { file =>
      //write body
      val localId = file.localId
      val crc32 = file.crc32
      val (offset: Long, length: Long, actualWritten: Long) = body.append(file.buf.nioBuffer(), crc32)
      //write metadata
      meta.write(localId, Constants.FILE_STATUS_MERGED, offset, length, crc32)
      localId
    }

    if (logger.isDebugEnabled)
      logger.debug(s"[region-${regionId}@$nodeId] merged: ${ids.toList}")
  }

  def flushAll(force: Boolean) = {
    if (force || mem.isDirtyEnough()) {
      this.synchronized {
        val f = fork()
        f.success()
      }
    }
  }

  def info = RegionInfo(nodeId, regionId, revision, isPrimary, isWritable, regionSize())
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

  def shutdown(): Unit = {
    regions.foreach(_._2.close())
  }

  def get(id: Long): Option[Region] = regions.get(id)

  def update(region: Region): Region = {
    val newRegion = new Region(nodeId, region.regionId, region.conf, listener)

    this.synchronized {
      regions(region.regionId) = newRegion
      //TODO: should close this object, but when?
      //region.close()
    }

    newRegion
  }

  private def _createNewRegion(regionId: Long) = {
    //create files
    val region = {
      //create a new region
      val regionDir = new File(storeDir, s"$regionId")
      regionDir.mkdirs()

      new File(regionDir, "body").createNewFile()
      new File(regionDir, "meta1").createNewFile()
      new File(regionDir, "meta2").createNewFile()
      new File(regionDir, "log").createNewFile()
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