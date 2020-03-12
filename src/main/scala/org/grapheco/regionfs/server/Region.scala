package org.grapheco.regionfs.server

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.concurrent.atomic.AtomicLong

import io.netty.buffer.{ByteBuf, Unpooled}
import org.grapheco.commons.util.Logging
import org.grapheco.regionfs.client.RegionFsClientException
import org.grapheco.regionfs.util.{Cache, CrcUtils, FixSizedCache}
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
case class MetaData(localId: Long, offset: Long, length: Long, crc32: Long) {
  def tail = offset + length
}

//[localId:Long][offset:Long][length:Long][crc32:Long][flag:Long]
class RegionMetaStore(conf: RegionConfig) extends Logging {
  lazy val fileMetaFile = new File(conf.regionDir, "meta")
  lazy val fptr = new RandomAccessFile(fileMetaFile, "rw");

  val cache: Cache[Long, MetaData] = new FixSizedCache[Long, MetaData](1024);

  def iterator(): Iterator[MetaData] = {
    (0 to cursor.current.toInt - 1).iterator.map(read(_).get)
  }

  //local id as offset
  val block = new Array[Byte](Constants.METADATA_ENTRY_LENGTH_WITH_PADDING);

  def read(localId: Long): Option[MetaData] = {
    if (localId >= cursor.current) {
      None
    }
    else {
      cache.get(localId).orElse {
        fptr.synchronized {
          fptr.seek(Constants.METADATA_ENTRY_LENGTH_WITH_PADDING * localId)
          fptr.readFully(block)
        }

        val dis = new DataInputStream(new ByteArrayInputStream(block))
        val info = MetaData(dis.readLong(), dis.readLong(), dis.readLong(), dis.readLong())
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
    dos.writeLong(offset)
    dos.writeLong(length)
    dos.writeLong(crc32)
    dos.writeByte(0) //flag,1=deleted
    //padding
    dos.write(new Array[Byte](Constants.METADATA_ENTRY_LENGTH_WITH_PADDING - dos.size()))

    fptr.synchronized {
      fptr.seek(Constants.METADATA_ENTRY_LENGTH_WITH_PADDING * localId)
      fptr.write(block.toByteArray)
    }

    dos.close()
    cache.put(localId, MetaData(localId, offset, length, crc32))
  }

  //since=0,tail=1
  def unsafeRead(since: Long, tail: Long): ByteBuffer = {
    fptr.synchronized {
      fptr.getChannel.map(FileChannel.MapMode.READ_ONLY,
        Constants.METADATA_ENTRY_LENGTH_WITH_PADDING * since,
        Constants.METADATA_ENTRY_LENGTH_WITH_PADDING * (tail - since));
    }
  }

  def unsafeWrite(buf: ByteBuffer): Unit = {
    fptr.synchronized {
      fptr.seek(fptr.length())
      fptr.getChannel.write(buf)
    }
  }

  def close(): Unit = {
    fptr.close()
  }

  val cursor = new Cursor(new AtomicLong(fileMetaFile.length() / Constants.METADATA_ENTRY_LENGTH_WITH_PADDING))
}

class Cursor(position: AtomicLong) {
  def offerNextId(consume: (Long) => Unit): Long = {
    position.synchronized {
      val id = position.get();
      consume(id)
      position.incrementAndGet()
    }
  }

  def current = position.get()

  def close(): Unit = {
  }
}

class RegionTrashStore(conf: RegionConfig) {
  private val fileBody = new File(conf.regionDir, "trash")
  val cursor = new AtomicLong(fileBody.length())
  lazy val readerChannel = new RandomAccessFile(fileBody, "r")
  lazy val appenderChannel = new FileOutputStream(fileBody, true).getChannel

  def length = readerChannel.length()

  def append(localId: Long): Unit = {
    val buf = ByteBuffer.allocate(1024)
    buf.putLong(localId)
    appenderChannel.synchronized {
      appenderChannel.write(buf)
    }
  }

  def close(): Unit = {
    appenderChannel.close()
    readerChannel.close()
  }

  val bytes = new Array[Byte](8 * 1024);

  def contains(localId: Long): Boolean = {
    if (length == 0)
      false
    else {
      var off = 0;
      var nread = 0;
      var found = false;
      //TODO: use page-cache
      readerChannel.synchronized {
        readerChannel.seek(0)
        do {
          nread = readerChannel.read(bytes, off, bytes.length)
          if (nread > 0) {
            off += nread;
            val dis = new DataInputStream(new ByteArrayInputStream(bytes, 0, nread))
            while (!found) {
              if (localId == dis.readLong) {
                found = true;
              }
            }
          }
        } while (nread > 0 && !found)
      }

      found
    }
  }
}

class RegionBodyStore(conf: RegionConfig) {
  //region file, one file for each region by far
  private val fileBody = new File(conf.regionDir, "body")
  val cursor = new AtomicLong(fileBody.length())
  lazy val readerChannel = new RandomAccessFile(fileBody, "r").getChannel
  lazy val appenderChannel = new FileOutputStream(fileBody, true).getChannel

  /**
    * @return (offset: Long, length: Long, actualWritten: Long)
    */
  def write(buf: ByteBuffer, crc: Long): (Long, Long, Long) = {
    val length = buf.remaining()
    val padding = new Array[Byte](Constants.REGION_FILE_ALIGNMENT_SIZE - 1 -
      (length + Constants.REGION_FILE_ALIGNMENT_SIZE - 1) % Constants.REGION_FILE_ALIGNMENT_SIZE)

    appenderChannel.synchronized {
      //7-(x+7)%8
      appenderChannel.write(Array(buf, ByteBuffer.wrap(padding.map(_ => '*'.toByte))))
    }

    val written = length + padding.length
    val offset = cursor.getAndAdd(written)

    if (conf.globalSetting.enableCrc) {
      val buf = read(offset, length)
      if (crc != CrcUtils.computeCrc32(buf)) {
        throw new WriteTimeMismatchedCheckSumException();
      }
    }

    (offset, length, written)
  }

  def unsafeWrite(buf: ByteBuffer): Unit = {
    appenderChannel.synchronized {
      appenderChannel.write(buf)
    }
  }

  def unsafeRead(offset: Long, tail: Long): ByteBuffer = {
    readerChannel.synchronized {
      readerChannel.map(FileChannel.MapMode.READ_ONLY, offset, tail - offset);
    }
  }

  def close(): Unit = {
    appenderChannel.close()
    readerChannel.close()
  }

  def read(offset: Long, length: Int): ByteBuffer = {
    readerChannel.synchronized {
      readerChannel.map(FileChannel.MapMode.READ_ONLY, offset, length);
    }
  }
}

/**
  * a Region store files in storeDir
  */
class Region(val nodeId: Int, val regionId: Long, val conf: RegionConfig, listener: RegionEventListener) extends Logging {
  //TODO: archive
  def isWritable = length <= conf.globalSetting.regionSizeLimit

  val isPrimary = (regionId >> 16) == nodeId

  //metadata file
  private lazy val fbody = new RegionBodyStore(conf)
  private lazy val fmeta = new RegionMetaStore(conf)
  private lazy val ftrash = new RegionTrashStore(conf)
  private lazy val cursor = fmeta.cursor

  def revision = cursor.current

  def fileCount = cursor.current

  def length = fbody.cursor.get()

  def peekNextFileId() = FileId(regionId, cursor.current)

  def listFiles(): Iterator[(FileId, Long)] = {
    fmeta.iterator.map(meta => FileId.make(regionId, meta.localId) -> meta.length)
  }

  def write(buf: ByteBuffer, crc: Long): (Long, Long) = {
    val crc32 =
      if (conf.globalSetting.enableCrc) {
        crc
      }
      else {
        0
      }

    //TODO: transaction assurance
    val (offset: Long, length: Long, actualWritten: Long) =
      fbody.write(buf, crc32)

    //get local id
    var localId = -1L
    cursor.offerNextId((id: Long) => {
      localId = id
      fmeta.write(id, offset, length, crc32)
      if (logger.isTraceEnabled())
        logger.trace(s"[region-${regionId}@${nodeId}] written: localId=$id, length=${length}, actual=${actualWritten}")
    })

    listener.handleRegionEvent(new WriteRegionEvent(this))
    localId -> revision
  }

  def close(): Unit = {
    fbody.close()
    fmeta.close()
    ftrash.close()
    cursor.close()
  }

  def read(localId: Long): Option[ByteBuffer] = {
    if (ftrash.contains(localId))
      None
    else {
      val maybeMeta = fmeta.read(localId)
      maybeMeta.map(meta => fbody.read(meta.offset, meta.length.toInt))
    }
  }

  def delete(localId: Long): Unit = {
    ftrash.append(localId)
  }

  def offerPatch(since: Long): ByteBuf = {
    val buf = Unpooled.buffer()

    val (rev, len) = this.synchronized {
      revision -> length
    }

    //1!=0
    if (rev == since) {
      buf.writeByte(Constants.MARK_GET_REGION_PATCH_ALREADY_UP_TO_DATE).writeLong(regionId)
      buf
    }
    else {
      //current version, length_of_metadata, body
      //write .metadata
      //write .body
      val meta1 = fmeta.read(since).get
      val metabuf = Unpooled.wrappedBuffer(fmeta.unsafeRead(since, rev))
      val bodybuf = Unpooled.wrappedBuffer(fbody.unsafeRead(meta1.offset, len))

      //TODO: checksum
      buf.writeByte(Constants.MARK_GET_REGION_PATCH_OK).
        writeLong(regionId).writeLong(rev).
        writeLong(metabuf.readableBytes()).
        writeLong(bodybuf.readableBytes())

      Unpooled.wrappedBuffer(buf, metabuf, bodybuf)
    }
  }

  //return value means if update is ok
  def applyPatch(is: InputStream): Boolean = {
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
        val regionId = dis.readLong()
        val lastRevision = dis.readLong()
        val sizeMeta = dis.readLong()
        val sizeBody = dis.readLong()

        val buf1 = Unpooled.buffer(1024)
        if (buf1.writeBytes(is, sizeMeta.toInt) != sizeMeta.toInt) {
          throw new WrongRegionPatchSizeException();
        }

        val buf2 = Unpooled.buffer(1024)
        if (buf2.writeBytes(is, sizeBody.toInt) != sizeBody.toInt) {
          throw new WrongRegionPatchSizeException();
        }

        this.synchronized {
          //TOOD: transaction assurance
          fmeta.unsafeWrite(buf1.nioBuffer())
          fbody.unsafeWrite(buf2.nioBuffer())
        }

        if (logger.isDebugEnabled)
          logger.debug(s"[region-${regionId}@${nodeId}] updated: .meta ${sizeMeta} bytes, .body ${sizeBody} bytes")

        true
    }
  }

  def status = RegionStatus(nodeId: Long, regionId: Long,
    revision: Long, isPrimary: Boolean, isWritable: Boolean, length: Long)
}

/**
  * RegionManager manages local regions stored in storeDir
  */
class RegionManager(nodeId: Int, storeDir: File, globalSetting: GlobalSetting, listener: RegionEventListener) extends Logging {
  val regions = mutable.Map[Long, Region]()
  val regionIdSerial = new AtomicLong(0)

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

  if (logger.isInfoEnabled())
    logger.info(s"[node-${nodeId}] loaded local regions: ${regions.keySet}")

  regionIdSerial.set((List(0L) ++ regions.map(_._1 >> 16).toList).max);

  def createNew() = {
    _createNewRegion((nodeId << 16) + regionIdSerial.incrementAndGet());
  }

  def createSecondaryRegion(regionId: Long) = {
    _createNewRegion(regionId);
  }

  def get(id: Long): Option[Region] = regions.get(id)

  def update(region: Region): Region = {
    region.close()

    val newRegion = new Region(nodeId, region.regionId, region.conf, listener)
    regions(region.regionId) = newRegion
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
        logger.trace(s"[region-${regionId}@${nodeId}] created: dir=$regionDir")

      new Region(nodeId, regionId, RegionConfig(regionDir, globalSetting), listener)
    }

    listener.handleRegionEvent(CreateRegionEvent(region))
    regions += (regionId -> region)
    region
  }
}

class WriteTimeMismatchedCheckSumException extends RegionFsServerException("mismatched checksum exception on write time") {

}

class ServerIsBudyException(nodeId: Int) extends RegionFsClientException(s"server is busy: ${nodeId}") {

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
  def handleRegionEvent(event: RegionEvent): Unit
}

case class WrongRegionPatchSizeException() extends RegionFsServerException(s"wrong region patch") {

}

case class RegionStatus(nodeId: Long, regionId: Long, revision: Long, isPrimary: Boolean, isWritable: Boolean, length: Long) {

}