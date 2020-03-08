package org.grapheco.regionfs.server

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.concurrent.atomic.AtomicLong

import io.netty.buffer.{ByteBuf, Unpooled}
import org.grapheco.commons.util.Logging
import org.grapheco.regionfs.client.RegionFsClientException
import org.grapheco.regionfs.util.{Cache, CrcUtils, FixSizedCache}
import org.grapheco.regionfs.{Constants, FileId, GlobalConfig}

import scala.collection.mutable

/**
  * Created by bluejoe on 2019/8/30.
  */
case class RegionConfig(regionDir: File, globalConfig: GlobalConfig) {

}

/**
  * metadata of a region
  */
case class MetaData(localId: Long, offset: Long, length: Long, crc32: Long) {
  def tail = offset + length
}

//[localId:Long][offset:Long][length:Long][crc32:Long][flag:Long]
class RegionMetaStore(conf: RegionConfig) {
  lazy val fileMetaFile = new File(conf.regionDir, "meta")
  lazy val fptr = new RandomAccessFile(fileMetaFile, "rw");

  val cache: Cache[Long, Option[MetaData]] = new FixSizedCache[Long, Option[MetaData]](1024);

  def iterator(): Iterator[MetaData] = {
    (0 to count.toInt - 1).iterator.map(read(_).get)
  }

  //local id as offset
  val block = new Array[Byte](Constants.METADATA_ENTRY_LENGTH_WITH_PADDING);

  def markDeleted(localId: Long): Unit = {
    fptr.synchronized {
      fptr.seek(Constants.METADATA_ENTRY_LENGTH_WITH_PADDING * localId + 4 * 8)
      fptr.writeByte(1)
    }
    cache.put(localId, None)
  }

  def copy(since: Long, tail: Long): ByteBuffer = {
    fptr.synchronized {
      fptr.getChannel.map(FileChannel.MapMode.READ_ONLY,
        Constants.METADATA_ENTRY_LENGTH_WITH_PADDING * since,
        Constants.METADATA_ENTRY_LENGTH_WITH_PADDING * (tail - since + 1));
    }
  }

  def read(localId: Long): Option[MetaData] = {
    cache.get(localId).getOrElse {
      fptr.synchronized {
        fptr.seek(Constants.METADATA_ENTRY_LENGTH_WITH_PADDING * localId)
        fptr.readFully(block)
      }

      val dis = new DataInputStream(new ByteArrayInputStream(block))
      val info = MetaData(dis.readLong(), dis.readLong(), dis.readLong(), dis.readLong())
      val entry =
      //deleted?
        if (dis.readByte() != 0) {
          None
        }
        else {
          Some(info)
        }
      dis.close()

      cache.put(localId, entry)
      entry
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

    cache.put(localId, Some(MetaData(localId, offset, length, crc32)))
  }

  def overwrite(buf: ByteBuffer): Unit = {
    fptr.synchronized {
      fptr.setLength(0)
      fptr.seek(0)
      fptr.getChannel.write(buf)
    }
  }

  def count = fileMetaFile.length() / Constants.METADATA_ENTRY_LENGTH_WITH_PADDING;

  def close(): Unit = {
    fptr.close()
    fptr.close()
  }
}

class LocalIdGenerator(conf: RegionConfig, meta: RegionMetaStore) {
  //free id
  val counterLocalId = new AtomicLong(meta.count);

  def consumeNextId(consume: (Long) => Unit): Long = {
    val id = counterLocalId.get();
    consume(id)
    counterLocalId.getAndIncrement()
  }

  def close(): Unit = {
  }
}

class RegionBodyStore(conf: RegionConfig) {
  //region file, one file for each region by far
  val fileBody = new File(conf.regionDir, "body")
  val fileBodyLength = new AtomicLong(fileBody.length())
  lazy val readerChannel = new RandomAccessFile(fileBody, "r").getChannel
  lazy val appenderChannel = new FileOutputStream(fileBody, true).getChannel

  /**
    * @return (offset: Long, length: Long, actualWritten: Long)
    */
  def write(buf: ByteBuffer, crc: Long): (Long, Long, Long) = {
    val length = buf.remaining()

    appenderChannel.synchronized {
      appenderChannel.write(Array(buf, ByteBuffer.wrap(Constants.REGION_FILE_BODY_EOF)))
    }

    val written = length + Constants.REGION_FILE_BODY_EOF_LENGTH
    val offset = fileBodyLength.getAndAdd(written)

    if (conf.globalConfig.enableCrc) {
      val buf = read(offset, length)
      if (crc != CrcUtils.computeCrc32(buf)) {
        throw new WriteTimeMismatchedCheckSumException();
      }
    }

    (offset, length, written)
  }

  def append(buf: ByteBuffer): Unit = {
    appenderChannel.synchronized {
      appenderChannel.write(buf)
    }
  }

  def close(): Unit = {
    appenderChannel.close()
    readerChannel.close()
  }

  def read(offset: Long, length: Int): ByteBuffer = {
    readerChannel.synchronized {
      readerChannel.position(offset)
      readerChannel.map(FileChannel.MapMode.READ_ONLY, offset, length);
    }
  }

  def copy(offset: Long, tail: Long): ByteBuffer = {
    readerChannel.synchronized {
      readerChannel.position(offset)
      readerChannel.map(FileChannel.MapMode.READ_ONLY, offset, tail - offset + 1);
    }
  }
}

/**
  * a Region store files in storeDir
  */
class Region(val nodeId: Int, val regionId: Long, val conf: RegionConfig, listener: RegionEventListener, val isPrimary: Boolean) extends Logging {
  val isWritable = true

  //metadata file
  lazy val fbody = new RegionBodyStore(conf)
  lazy val fmeta = new RegionMetaStore(conf)
  lazy val idgen = new LocalIdGenerator(conf, fmeta)

  def revision = idgen.counterLocalId.get()

  def length = fbody.fileBodyLength.get()

  //TOOD: deleted items
  def statFileCount(): Long = {
    fmeta.count
  }


  def listFiles(): Iterator[(FileId, Long)] = {
    fmeta.iterator.map(meta => FileId.make(regionId, meta.localId) -> meta.length)
  }

  def write(buf: ByteBuffer, crc: Long): Long = {
    val crc32 =
      if (conf.globalConfig.enableCrc) {
        crc
      }
      else {
        0
      }

    //TODO: transaction assurance
    val (offset: Long, length: Long, actualWritten: Long) =
      fbody.write(buf, crc32)

    //get local id
    idgen.consumeNextId((id: Long) => {
      fmeta.write(id, offset, length, crc32)
      if (logger.isTraceEnabled())
        logger.trace(s"[region-${regionId}@${nodeId}] written: localId=$id, length=${length}, actual=${actualWritten}")

      listener.handleRegionEvent(new WriteRegionEvent(this))
    })
  }

  def close(): Unit = {
    fbody.close()
    fmeta.close()
    idgen.close()
  }

  def read(localId: Long): Option[ByteBuffer] = {
    val maybeMeta = fmeta.read(localId)
    maybeMeta.map(meta => fbody.read(meta.offset, meta.length.toInt))
  }

  def applyPatch(is: InputStream): Unit = {
    val dis = new DataInputStream(is)
    val mark = dis.readByte()
    mark match {
      case Constants.MARK_GET_REGION_PATCH_ALREADY_UP_TO_DATE =>

      case Constants.MARK_GET_REGION_PATCH_SERVER_IS_BUSY =>
        val regionId = dis.readLong()
        if (logger.isDebugEnabled)
          logger.debug(s"server is busy now: ${regionId >> 16}")

      case Constants.MARK_GET_REGION_PATCH_OK =>
        val regionId = dis.readLong()
        val lastRevision = dis.readLong()
        val lenMeta = dis.readLong()
        val lenBody = dis.readLong()

        val buf2 = Unpooled.buffer(1024)
        buf2.writeBytes(is, lenBody.toInt)
        fbody.append(buf2.nioBuffer())

        val buf1 = Unpooled.buffer(1024)
        buf1.writeBytes(is, lenMeta.toInt)
        fmeta.overwrite(buf1.nioBuffer())

        if (logger.isDebugEnabled)
          logger.debug(s"[region-${regionId}@${nodeId}] updated: .meta ${lenMeta} bytes, .body ${lenBody} bytes")
    }
  }

  def buildPatch(since: Long): ByteBuf = {
    val bytebuf = Unpooled.compositeBuffer()
    val rev = revision

    if (rev == since) {
      bytebuf.writeByte(Constants.MARK_GET_REGION_PATCH_ALREADY_UP_TO_DATE).writeLong(regionId)
    }
    else {
      //current version, length_of_metadata, body
      //write .metadata
      //write .body
      val meta1 = fmeta.read(since).get
      val meta2 = fmeta.read(rev).get

      val metabuf = Unpooled.wrappedBuffer(fmeta.copy(since, rev))
      val bodybuf = Unpooled.wrappedBuffer(fbody.copy(meta1.offset, meta2.tail))

      bytebuf.writeByte(Constants.MARK_GET_REGION_PATCH_OK).writeLong(regionId).writeLong(rev).writeLong(metabuf.readableBytes()).writeLong(bodybuf.readableBytes())

      bytebuf.addComponent(metabuf)
      bytebuf.addComponent(bodybuf)
    }
    bytebuf
  }

  def delete(localId: Long): Unit = {
    fmeta.markDeleted(localId)
  }
}

/**
  * RegionManager manages local regions stored in storeDir
  */
class RegionManager(nodeId: Int, storeDir: File, globalConfig: GlobalConfig, listener: RegionEventListener) extends Logging {
  val regions = mutable.Map[Long, Region]()
  lazy val regionIdSerial = new AtomicLong(0)

  def get(id: Long): Option[Region] = regions.get(id)

  def reload(region: Region): Region = {
    val newRegion = new Region(nodeId, region.regionId, region.conf, listener, region.isPrimary)
    regions(region.regionId) = newRegion
    newRegion
  }

  private def isPrimary(id: Long): Boolean = (id >> 16) == nodeId;
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
      id -> new Region(nodeId, id, RegionConfig(file, globalConfig), listener, isPrimary(id))
    }

  if (logger.isInfoEnabled())
    logger.info(s"[node-${nodeId}] loaded local regions: ${regions.keySet}")

  regionIdSerial.set((List(0L) ++ regions.map(_._1 >> 16).toList).max);

  def createNew() = {
    _createNewRegion((nodeId << 16) + regionIdSerial.incrementAndGet());
  }

  def createNewReplica(regionId: Long) = {
    _createNewRegion(regionId);
  }

  private def _createNewRegion(regionId: Long) = {
    //create files
    val region = {
      //create a new region
      val regionDir = new File(storeDir, s"$regionId")
      regionDir.mkdir()
      //region file, one file for each region by far
      val fileBody = new File(regionDir, "body")
      fileBody.createNewFile()
      //metadata file
      val fileMeta = new File(regionDir, "meta")
      fileMeta.createNewFile()

      if (logger.isTraceEnabled())
        logger.trace(s"[region-${regionId}@${nodeId}] created: dir=$regionDir")

      new Region(nodeId, regionId, RegionConfig(regionDir, globalConfig), listener, isPrimary(regionId))
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

object Region {
  def unpack(nodeId: Long, regionId: Long, data: Array[Byte]): RegionData = {
    val bais = new ByteArrayInputStream((data))
    val dis = new DataInputStream(bais)
    RegionData(nodeId, regionId, dis.readLong(), dis.readBoolean(), dis.readBoolean(), dis.readLong())
  }

  def pack(region: Region): Array[Byte] = {
    val baos = new ByteArrayOutputStream();
    val dos = new DataOutputStream(baos);
    dos.writeLong(region.revision)
    dos.writeBoolean(region.isPrimary)
    dos.writeBoolean(region.isWritable)
    dos.writeLong(region.length)

    dos.close()
    baos.toByteArray
  }
}

case class RegionData(nodeId: Long, regionId: Long, revision: Long, isPrimary: Boolean, isWritable: Boolean, length: Long) {
}
