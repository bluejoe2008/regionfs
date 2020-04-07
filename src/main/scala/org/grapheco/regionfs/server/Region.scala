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

/**
  * metadata of a region
  */
case class FileMetadata(localId: Long, status: Byte, offset: Long, length: Long, crc32: Long) {
  def tail = offset + length
}

//[localId:Long][status:Byte][offset:Long][length:Long][crc32:Long]
//TODO: use 2-level metadata to avoid unused localIds
//level1: [offset:Long]
//level2: [localId:Long][status:Byte][offset:Long][length:Long][crc32:Long]
class RegionMetadataStore(conf: RegionConfig) extends Logging {

  lazy val fileMetaFile = new File(conf.regionDir, "meta")
  lazy val fptr = new RandomAccessFile(fileMetaFile, "rw")

  val cache: Cache[Long, FileMetadata] = new FixSizedCache[Long, FileMetadata](1024)

  def entries(): Iterable[FileMetadata] = {
    (0 to count().toInt - 1).map(read(_).get)
  }

  val metadataBlock = new Array[Byte](Constants.METADATA_ENTRY_LENGTH_WITH_PADDING)

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
          fptr.seek(Constants.METADATA_ENTRY_LENGTH_WITH_PADDING * localId)
          fptr.readFully(metadataBlock)
        }

        val dis = new DataInputStream(new ByteArrayInputStream(metadataBlock))
        val info = FileMetadata(dis.readLong(), dis.readByte(), dis.readLong(), dis.readLong(), dis.readLong())
        dis.close()

        //if (info.status == Constants.FILE_STATUS_GLOBAL_WRITTEN || info.status == Constants.FILE_STATUS_WRITTEN_IN_PLACE) {
        cache.put(localId, info)
        //}

        Some(info)
      }
    }
  }

  def write(localId: Long, status: Byte, offset: Long, length: Long, crc32: Long): Unit = {
    val block = new ByteArrayOutputStream()
    val dos = new DataOutputStream(block)
    dos.writeLong(localId)
    dos.writeByte(status)
    dos.writeLong(offset)
    dos.writeLong(length)
    dos.writeLong(crc32)
    //padding
    dos.write(new Array[Byte](Constants.METADATA_ENTRY_LENGTH_WITH_PADDING - dos.size()))

    fptr.synchronized {
      //maybe overwrite
      fptr.seek(Constants.METADATA_ENTRY_LENGTH_WITH_PADDING * localId)
      fptr.write(block.toByteArray)
    }

    dos.close()

    //if (status == Constants.FILE_STATUS_GLOBAL_WRITTEN || status == Constants.FILE_STATUS_WRITTEN_IN_PLACE)
    cache.put(localId, FileMetadata(localId, status, offset, length, crc32))
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
      if (meta.status == Constants.FILE_STATUS_GLOBAL_WRITTEN || meta.status == Constants.FILE_STATUS_WRITTEN_IN_PLACE) {
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

class RegionFileBuffer(conf: RegionConfig) {
  def read(localId: Long, length: Long): ByteBuffer = {
    assert(length >= 0)
    val file = new File(conf.regionDir, s"$localId")
    val fptr = new RandomAccessFile(file, "r")
    val buf = fptr.getChannel.map(FileChannel.MapMode.READ_ONLY, 0, length)
    fptr.close()
    buf
  }
}

/**
  * a Region store files in storeDir
  */
class Region(val nodeId: Int, val regionId: Long, val conf: RegionConfig, listener: RegionEventListener) extends Logging {
  val isPrimary = (regionId >> 16) == nodeId
  val isSecondary = !isPrimary

  //metadata file
  private lazy val fbuffer = new RegionFileBuffer(conf)
  private lazy val fbody = new RegionBodyStore(conf)
  private lazy val fmeta = new RegionMetadataStore(conf)
  private lazy val ftrash = new RegionTrashStore(conf)

  def revision() = fmeta.count() //fids.current

  def fileCount() = fmeta.count() - ftrash.count()

  def bodyLength() = fbody.fptr.length()

  //TODO: archive
  def isWritable = bodyLength <= conf.globalSetting.regionSizeLimit

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
    Rollbackable.success(fmeta.createNextId()) {

    }
  }

  def markGlobalWriten(localId: Long, length: Long): Rollbackable = {
    fmeta.write(localId, Constants.FILE_STATUS_GLOBAL_WRITTEN, -1, length, 0)
    Rollbackable.success(localId) {

    }
  }

  def markLocalWriten(localId: Long): Rollbackable = {
    fmeta.write(localId, Constants.FILE_STATUS_LOCAL_WRITTEN, -1, -1, 0)
    Rollbackable.success(localId) {

    }
  }

  def saveLocalFile(localId: Long, buf: ByteBuffer, crc32: Long): Rollbackable = {
    //save temp file
    fmeta.write(localId, Constants.FILE_STATUS_TO_WRITE, -1, -1, 0)

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
      maybeMeta.map(meta =>
        meta.status match {
          case Constants.FILE_STATUS_GLOBAL_WRITTEN =>
            fbuffer.read(localId, meta.length)
          case Constants.FILE_STATUS_WRITTEN_IN_PLACE =>
            fbody.read(meta.offset, meta.length)
        }
      )
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
      val metas = fmeta.offerMetaPatch(sinceRevision)

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

          (Transactional {
            case _ =>
              this.saveLocalFile(localId, buf.nioBuffer(), crc32)
          } & {
            case _ =>
              this.markGlobalWriten(localId, fileSize)
          }).perform(1, TransactionalContext.DEFAULT)
        }

        true
    }
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