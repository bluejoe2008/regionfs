package org.grapheco.regionfs.server

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.concurrent.atomic.AtomicLong
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
    (0 to cursor.current.toInt - 1).iterator.map(read(_).get)
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

  def close(): Unit = {
    fptr.close()
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

    appenderChannel.synchronized {
      appenderChannel.write(Array(buf, ByteBuffer.wrap(Constants.REGION_FILE_BODY_EOF)))
    }

    val written = length + Constants.REGION_FILE_BODY_EOF_LENGTH
    val offset = cursor.getAndAdd(written)

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
}

/**
  * a Region store files in storeDir
  */
class Region(val nodeId: Int, val regionId: Long, val conf: RegionConfig, listener: RegionEventListener) extends Logging {
  //TODO: archive
  def isWritable = length <= conf.globalConfig.regionSizeLimit
  val isPrimary = (regionId >> 16) == nodeId

  //metadata file
  private lazy val fbody = new RegionBodyStore(conf)
  private lazy val fmeta = new RegionMetaStore(conf)
  private lazy val cursor = fmeta.cursor

  def revision = cursor.current

  def fileCount = cursor.current

  def length = fbody.cursor.get()

  def peekNextFileId() = FileId(regionId, cursor.current)

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
    var localId = -1L
    cursor.offerNextId((id: Long) => {
      localId = id
      fmeta.write(id, offset, length, crc32)
      if (logger.isTraceEnabled())
        logger.trace(s"[region-${regionId}@${nodeId}] written: localId=$id, length=${length}, actual=${actualWritten}")
    })

    //listener.handleRegionEvent(new WriteRegionEvent(this))
    localId
  }

  def close(): Unit = {
    fbody.close()
    fmeta.close()
    cursor.close()
  }

  def read(localId: Long): Option[ByteBuffer] = {
    val maybeMeta = fmeta.read(localId)
    maybeMeta.map(meta => fbody.read(meta.offset, meta.length.toInt))
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
      id -> new Region(nodeId, id, RegionConfig(file, globalConfig), listener)
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

  def get(id: Long): Option[Region] = regions.get(id)

  def reload(region: Region): Region = {
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
      //region file, one file for each region by far
      val fileBody = new File(regionDir, "body")
      fileBody.createNewFile()
      //metadata file
      val fileMeta = new File(regionDir, "meta")
      fileMeta.createNewFile()

      if (logger.isTraceEnabled())
        logger.trace(s"[region-${regionId}@${nodeId}] created: dir=$regionDir")

      new Region(nodeId, regionId, RegionConfig(regionDir, globalConfig), listener)
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
