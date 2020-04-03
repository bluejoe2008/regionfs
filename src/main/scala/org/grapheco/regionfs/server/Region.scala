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

/**
  * Created by bluejoe on 2019/8/30.
  */
case class RegionConfig(regionDir: File, globalSetting: GlobalSetting) {

}

/**
  * metadata of a region
  */
case class FileMetadata(localId: Long, status: Byte, offset: Long, length: Long, crc32: Long) {
  def tail = offset + length
}

class RegionLocalIdStore(conf: RegionConfig) extends Logging {
  val file = new File(conf.regionDir, "id")
  val idgen = if (file.length() == 0) {
    new AtomicLong(0)
  }
  else {
    val dis = new DataInputStream(new FileInputStream(file))
    val current = dis.readLong()
    dis.close()
    new AtomicLong(current)
  }

  def current(): Long = idgen.get()

  def nextId(): Long = {
    val nid = idgen.incrementAndGet()
    //TODO: batch generation
    val dos = new DataOutputStream(new FileOutputStream(file, false))
    dos.writeLong(nid)
    dos.close()
    nid
  }
}

//[localId:Long][offset:Long][length:Long][crc32:Long][flag:Long]
class RegionMetadataStore(conf: RegionConfig) extends Logging {
  lazy val fileMetaFile = new File(conf.regionDir, "meta")
  lazy val fptr = new RandomAccessFile(fileMetaFile, "rw")

  val cache: Cache[Long, FileMetadata] = new FixSizedCache[Long, FileMetadata](1024)

  def markStatus(localId: Long, status: Byte): Unit = {
    val block = new ByteArrayOutputStream()
    val dos = new DataOutputStream(block)
    dos.writeLong(localId)
    dos.writeByte(status)

    fptr.synchronized {
      //extends if needed
      fptr.seek(Constants.METADATA_ENTRY_LENGTH_WITH_PADDING * localId)
      fptr.write(block.toByteArray)
    }
  }

  def entries(): Iterable[FileMetadata] = {
    (0 to count().toInt - 1).map(read(_).get)
  }

  val block = new Array[Byte](Constants.METADATA_ENTRY_LENGTH_WITH_PADDING)

  def read(localId: Long): Option[FileMetadata] = {
    if (localId >= count()) {
      None
    }
    else {
      cache.get(localId).orElse {
        fptr.synchronized {
          fptr.seek(Constants.METADATA_ENTRY_LENGTH_WITH_PADDING * localId)
          fptr.readFully(block)
        }

        val dis = new DataInputStream(new ByteArrayInputStream(block))
        val info = FileMetadata(dis.readLong(), dis.readByte(), dis.readLong(), dis.readLong(), dis.readLong())
        dis.close()

        cache.put(localId, info)
        Some(info)
      }
    }
  }

  def write(localId: Long, offset: Long, length: Long, crc32: Long): Unit = {
    val block = new ByteArrayOutputStream()
    val dos = new DataOutputStream(block)
    dos.writeLong(localId)
    dos.writeByte(0)
    dos.writeLong(offset)
    dos.writeLong(length)
    dos.writeLong(crc32)
    //padding
    dos.write(new Array[Byte](Constants.METADATA_ENTRY_LENGTH_WITH_PADDING - dos.size()))

    fptr.synchronized {
      fptr.seek(Constants.METADATA_ENTRY_LENGTH_WITH_PADDING * localId)
      fptr.write(block.toByteArray)
    }

    dos.close()
    cache.put(localId, FileMetadata(localId, 0, offset, length, crc32))
  }

  //since=0,tail=1
  def offerMetaPatch(sinceRevision: Long): (Long, Long, Long, ByteBuffer) = {
    //since=10, tail=13
    val sinceMetaFile = Constants.METADATA_ENTRY_LENGTH_WITH_PADDING * sinceRevision
    var bodyPatchSize = 0L
    val tail = count()
    assert(sinceRevision < tail)

    val sinceBodyFile = read(sinceRevision).get.offset
    var n = sinceRevision

    while (n < tail && bodyPatchSize < Constants.MAX_PATCH_SIZE) {
      val meta = read(n).get
      n += 1
      bodyPatchSize = meta.tail - sinceBodyFile
    }

    //n=13
    (sinceMetaFile, sinceBodyFile, bodyPatchSize, fptr.synchronized {
      fptr.getChannel.map(FileChannel.MapMode.READ_ONLY,
        sinceMetaFile,
        Constants.METADATA_ENTRY_LENGTH_WITH_PADDING * (n - sinceRevision))
    })
  }

  def applyMetaPatch(buf: ByteBuffer, since: Long, size: Long, crc: Long): Unit = {
    assert(size == buf.remaining())

    fptr.synchronized {
      fptr.seek(since)
      fptr.getChannel.write(buf)
    }

    fptr.seek(since)
    val buf2 = fptr.getChannel.map(FileChannel.MapMode.READ_ONLY, since, size)
    assert(crc == CrcUtils.computeCrc32(buf2))
  }

  def close(): Unit = {
    fptr.close()
  }

  def count() = fptr.length() / Constants.METADATA_ENTRY_LENGTH_WITH_PADDING
}

class RegionTrashStore(conf: RegionConfig) {
  private val fileBody = new File(conf.regionDir, "trash")

  class Cache {
    val _ids = mutable.Map[Long, Byte]();
    {
      var eof = false
      val dis = new DataInputStream(new BufferedInputStream(new FileInputStream(fileBody)))
      while (!eof) {
        try {
          _ids += dis.readLong() -> 1
        }
        catch {
          case e: EOFException =>
            eof = true
        }
      }

      dis.close()
    }

    def append(id: Long): Boolean = this.synchronized {
      if (_ids.contains(id)) {
        false
      }
      else {
        _ids += id -> 1
        true
      }
    }
  }

  lazy val cache = new Cache()
  lazy val appender = new DataOutputStream(new FileOutputStream(fileBody, true))

  def count() = cache._ids.size

  def append(localId: Long): Unit = {
    if (cache.append(localId)) {
      appender.synchronized {
        appender.writeLong(localId)
      }
    }
  }

  def close(): Unit = {
    appender.close()
  }

  val bytes = new Array[Byte](8 * 1024)

  def contains(localId: Long): Boolean = {
    cache._ids.contains(localId)
  }
}

class RegionBodyStore(conf: RegionConfig) {
  //region file, one file for each region by far
  private val fileBody = new File(conf.regionDir, "body")
  lazy val fptr = new RandomAccessFile(fileBody, "rw")

  /**
    * @return (offset: Long, length: Long, actualWritten: Long)
    */
  def write(buf: ByteBuffer, crc: Long): (Long, Long, Long) = {
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

  def applyBodyPatch(buf: ByteBuffer, since: Long, size: Long, crc: Long): Unit = {
    assert(size == buf.remaining())

    fptr.synchronized {
      fptr.seek(since)
      fptr.getChannel.write(buf)
    }

    fptr.seek(since)
    val buf2 = fptr.getChannel.map(FileChannel.MapMode.READ_ONLY, since, size)
    assert(crc == CrcUtils.computeCrc32(buf2))
  }

  def offerBodyPatch(offset: Long, length: Long): ByteBuffer = {
    fptr.synchronized {
      fptr.getChannel.map(FileChannel.MapMode.READ_ONLY, offset, length)
    }
  }

  def close(): Unit = {
    fptr.close()
  }

  def read(offset: Long, length: Int): ByteBuffer = {
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

  //metadata file
  private lazy val fbody = new RegionBodyStore(conf)
  private lazy val fmeta = new RegionMetadataStore(conf)
  private lazy val ftrash = new RegionTrashStore(conf)
  private lazy val fids = new RegionLocalIdStore(conf)

  def revision() = fids.current

  def fileCount() = fmeta.count() - ftrash.count()

  def length() = fbody.fptr.length()

  //TODO: archive
  def isWritable = length <= conf.globalSetting.regionSizeLimit

  def listFiles(): Iterable[FileEntry] = {
    fmeta.entries().map { meta =>
      new FileEntry() {
        override val id = FileId.make(regionId, meta.localId)
        override val length = meta.length
        override val region = info

        override def process[T](f: (InputStream) => T) = {
          val is = new ByteBufferInputStream(read(meta.localId).get)
          val t = f(is)
          is.close()
          t
        }
      }
    }
  }

  def createLocalId(): Rollbackable = {
    Rollbackable.success(fids.nextId()) {}
  }

  private def _markStatus(localId: Long, flag: Byte): Rollbackable = {
    fmeta.markStatus(localId, flag)
    Rollbackable.success(localId) {}
  }

  def markGlobalWriten(localId: Long): Rollbackable = {
    _markStatus(localId, Constants.FILE_STATUS_GLOBAL_WRITTEN)
  }

  def markLocalWriten(localId: Long): Rollbackable = {
    _markStatus(localId, Constants.FILE_STATUS_LOCAL_WRITTEN)
  }

  def saveLocalFile(localId: Long, buf: ByteBuffer, crc32: Long): Rollbackable = {
    //save temp file
    _markStatus(localId, Constants.FILE_STATUS_TO_WRITE)

    val file = new File(conf.regionDir, s"$localId");
    val rbuf = buf.duplicate()
    val fptr = new RandomAccessFile(file, "rw")
    fptr.getChannel.write(buf.duplicate())

    fptr.getChannel.map(FileChannel.MapMode.READ_ONLY, 0, fptr.length())
    fptr.close()

    val crc2 = CrcUtils.computeCrc32(rbuf)
    if (crc32 != crc2) {
      Rollbackable.failure(new WrittenMismatchedStreamException())
    }
    else {
      Rollbackable.success(localId) {
        file.delete()
      }
    }
  }

  def close(): Unit = {
    fbody.close()
    fmeta.close()
    ftrash.close()
  }

  def read(localId: Long): Option[ByteBuffer] = {
    if (ftrash.contains(localId))
      None
    else {
      val maybeMeta = fmeta.read(localId)
      maybeMeta.map(meta => fbody.read(meta.offset, meta.length.toInt))
    }
  }

  def delete(localId: Long): Boolean = {
    if (fmeta.read(localId).isDefined && !ftrash.contains(localId)) {
      ftrash.append(localId)
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
      //current version, length_of_metadata, body
      //write .metadata
      //write .body
      val (sinceMeta, sinceBody, lengthBody, buf1) = fmeta.offerMetaPatch(sinceRevision)
      assert(lengthBody != 0)
      val metabuf = Unpooled.wrappedBuffer(buf1)
      val buf2: ByteBuffer = fbody.offerBodyPatch(sinceBody, lengthBody)
      val bodybuf = Unpooled.wrappedBuffer(buf2)

      val crc1 = CrcUtils.computeCrc32(buf1)
      val crc2 = CrcUtils.computeCrc32(buf2)

      buf.writeByte(Constants.MARK_GET_REGION_PATCH_OK).
        writeLong(regionId).writeLong(rev).
        writeLong(sinceMeta).writeLong(sinceBody).
        writeLong(metabuf.readableBytes()).writeLong(bodybuf.readableBytes()).
        writeLong(crc1).writeLong(crc2)

      //composite buffer costs more time!!!
      //Unpooled.wrappedBuffer(buf, metabuf, bodybuf)
      buf.writeBytes(metabuf).writeBytes(bodybuf)
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
        val (regionId, _, sinceMeta, sinceBody, sizeMeta, sizeBody, crc1, crc2) =
          (
            dis.readLong(), dis.readLong(), dis.readLong(),
            dis.readLong(), dis.readLong(), dis.readLong(),
            dis.readLong(), dis.readLong()
            )

        val buf1 = Unpooled.buffer(1024)
        if (buf1.writeBytes(is, sizeMeta.toInt) != sizeMeta.toInt) {
          throw new WrongRegionPatchSizeException()
        }

        if (crc1 != CrcUtils.computeCrc32(buf1.nioBuffer())) {
          throw new WrongRegionPatchStreamException()
        }

        val buf2 = Unpooled.buffer(1024)
        if (buf2.writeBytes(is, sizeBody.toInt) != sizeBody.toInt) {
          throw new WrongRegionPatchSizeException()
        }

        if (crc2 != CrcUtils.computeCrc32(buf2.nioBuffer())) {
          throw new WrongRegionPatchStreamException()
        }

        this.synchronized {
          //TOOD: transaction assurance
          fmeta.applyMetaPatch(buf1.nioBuffer(), sinceMeta, sizeMeta, crc1)
          fbody.applyBodyPatch(buf2.nioBuffer(), sinceBody, sizeBody, crc2)

          if (logger.isDebugEnabled)
            logger.debug(s"[region-$regionId@$nodeId] updated: .meta $sizeMeta bytes, .body $sizeBody bytes")

          callbackOnUpdated
        }

        true
    }
  }

  def info = RegionInfo(nodeId, regionId, revision, isPrimary, isWritable, length)
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
      new File(regionDir, "id").createNewFile()
      new File(regionDir, "trash").createNewFile()

      if (logger.isTraceEnabled())
        logger.trace(s"[region-$regionId@$nodeId] created: dir=$regionDir")

      new Region(nodeId, regionId, RegionConfig(regionDir, globalSetting), listener)
    }

    listener.handleRegionEvent(CreateRegionEvent(region))
    regions += (regionId -> region)
    ring += regionId
    region
  }
}

class WrittenMismatchedStreamException extends RegionFsServerException("mismatched checksum exception on write time") {

}

class ServerIsBudyException(nodeId: Int) extends RegionFsClientException(s"server is busy: $nodeId") {

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

trait FileEntry {
  val id: FileId;
  val region: RegionInfo;
  val length: Long;

  def process[T](f: (InputStream) => T): T;
}