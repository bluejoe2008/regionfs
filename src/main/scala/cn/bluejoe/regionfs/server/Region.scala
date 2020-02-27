package cn.bluejoe.regionfs.server

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.{CRC32, CheckedInputStream}

import cn.bluejoe.regionfs.util.{Cache, FixSizedCache}
import cn.bluejoe.regionfs.{Constants, FileId, GlobalConfig}
import cn.bluejoe.util.{ByteBufferInputStream, Logging}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

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

class RegionMetaStore(conf: RegionConfig) {
  val fileMetaFile = new File(conf.regionDir, "meta")
  val writer = new RandomAccessFile(fileMetaFile, "rw");
  val reader = new RandomAccessFile(fileMetaFile, "r");

  val cache: Cache[Long, MetaData] = new FixSizedCache[Long, MetaData](1024);

  def iterator(): Iterator[MetaData] = {
    (0 to count.toInt - 1).iterator.map(read(_))
  }

  //local id as offset
  val block = new Array[Byte](Constants.METADATA_ENTRY_LENGTH_WITH_PADDING);

  def read(localId: Long): MetaData = {
    cache.get(localId).getOrElse {
      reader.seek(Constants.METADATA_ENTRY_LENGTH_WITH_PADDING * localId)
      reader.readFully(block)

      val dis = new DataInputStream(new ByteArrayInputStream(block))
      val info = MetaData(dis.readLong(), dis.readLong(), dis.readLong(), dis.readLong())

      dis.close()
      info
    }
  }

  def write(localId: Long, offset: Long, length: Long, crc32: Long): Unit = {
    //[iiii][iiii][oooo][oooo][llll][llll][cccc][cccc]
    val block = new ByteArrayOutputStream()
    val dos = new DataOutputStream(block)
    dos.writeLong(localId)
    dos.writeLong(offset)
    dos.writeLong(length)
    dos.writeLong(crc32)
    dos.writeLong(0) //reserved

    writer.seek(Constants.METADATA_ENTRY_LENGTH_WITH_PADDING * localId)
    writer.write(block.toByteArray)
    dos.close()

    cache.put(localId, MetaData(localId, offset, length, crc32))
  }

  def count = fileMetaFile.length() / Constants.METADATA_ENTRY_LENGTH_WITH_PADDING;

  def close(): Unit = {
    reader.close()
    writer.close()
  }
}

class FreeIdStore(conf: RegionConfig) {
  val freeIdFile = new File(conf.regionDir, "freeid")
  val writer = new FileOutputStream(freeIdFile, false);

  val freeIds = {
    //FIXME: read all bytes!
    val bytes = new Array[Byte](freeIdFile.length().toInt);
    val raf = new RandomAccessFile(freeIdFile, "r");
    raf.readFully(bytes)
    raf.close()

    val ids = new ArrayBuffer[Long]();
    val dis = new DataInputStream(new ByteArrayInputStream(bytes))
    breakable {
      while (true) {
        try {
          ids += dis.readLong()
        }
        catch {
          case _: Throwable => break
        }
      }
    }

    ids
  }

  def consumeNextId(consume: (Long) => Unit): Option[Long] = {
    if (freeIds.isEmpty)
      None
    else {
      val id = freeIds.head
      consume(id)
      freeIds.remove(0)
      flush()

      Some(id)
    }
  }

  def addFreeId(id: Long): Unit = {
    freeIds += id
    flush()
  }

  private def flush(): Unit = {
    val block = new ByteArrayOutputStream()
    val dos = new DataOutputStream(block)
    freeIds.foreach(dos.writeLong(_))
    writer.write(block.toByteArray)
    dos.close()
  }
}

class LocalIdGenerator(conf: RegionConfig, meta: RegionMetaStore) {
  //free id
  val counterLocalId = new AtomicLong(meta.count);
  val freeId = new FreeIdStore(conf)

  def consumeNextId(consume: (Long) => Unit): Long = {
    freeId.consumeNextId(consume).getOrElse {
      val id = counterLocalId.get();
      consume(id)
      counterLocalId.getAndIncrement()
    }
  }

  def close(): Unit = {
  }
}

case class WriteInfo(offset: Long, length: Long, actualWritten: Long) {

}

class RegionBodyStore(conf: RegionConfig) {
  //region file, one file for each region by far
  val fileBody = new File(conf.regionDir, "body")
  val fileBodyLength = new AtomicLong(fileBody.length())

  val readerChannel = new RandomAccessFile(fileBody, "r").getChannel
  val appenderChannel = new FileOutputStream(fileBody, true).getChannel

  def write(buf: ByteBuffer): WriteInfo = {
    val length = buf.remaining()

    appenderChannel.synchronized {
      appenderChannel.write(Array(buf, ByteBuffer.wrap(Constants.REGION_FILE_BODY_EOF)))
    }

    val written = length + Constants.REGION_FILE_BODY_EOF.length
    WriteInfo(fileBodyLength.getAndAdd(written), length, written)
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
class Region(val replica: Boolean, val regionId: Long, conf: RegionConfig) extends Logging {
  //metadata file
  val fbody = new RegionBodyStore(conf)
  val fmeta = new RegionMetaStore(conf)
  val idgen = new LocalIdGenerator(conf, fmeta)

  def statFileCount(): Long = {
    fmeta.count - idgen.freeId.freeIds.size
  }

  private def computeCrc32(getInputStream: () => InputStream): Long = {
    //get crc32
    val crc32 = new CRC32()
    val cis = new CheckedInputStream(getInputStream(), crc32)
    while (cis.read() != -1) {
    }
    val crc32Value = crc32.getValue
    cis.close()
    crc32Value
  }

  def statTotalSize() = fbody.fileBodyLength.get()

  def listFiles(): Iterator[(FileId, Long)] = {
    fmeta.iterator.map(meta => FileId.make(regionId, meta.localId) -> meta.length)
  }

  def write(buf: ByteBuffer): Long = {
    val info = fbody.write(buf)
    val crc32 =
      if (conf.globalConfig.enableCrc) {
        val clone = buf.duplicate()
        computeCrc32(() => new ByteBufferInputStream(clone))
      }
      else {
        0
      }

    //get local id
    idgen.consumeNextId((id: Long) => {
      fmeta.write(id, info.offset, info.length, crc32)
      if (logger.isTraceEnabled())
        logger.trace(s"[region-$regionId] written:localId=$id, length=${info.length}, actual=${info.actualWritten}")
    })
  }

  def close(): Unit = {
    fbody.close()
    fmeta.close()
    idgen.close()
  }

  def read(localId: Long): ByteBuffer = {
    val meta = fmeta.read(localId)
    fbody.read(meta.offset, meta.length.toInt)
  }
}

/**
  * RegionManager manages local regions stored in storeDir
  */
//TODO: few live regions + most dead regions
class RegionManager(nodeId: Long, storeDir: File, globalConfig: GlobalConfig) extends Logging {
  val regions = mutable.Map[Long, Region]()
  val regionIdSerial = new AtomicLong(0)

  def get(id: Long) = regions(id)

  private def isReplica(id: Long): Boolean = (id >> 16) != nodeId;
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
      id -> new Region(isReplica(id), id, RegionConfig(file, globalConfig))
    }

  if (logger.isInfoEnabled())
    logger.info(s"loaded local regions: ${regions.keySet}")

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
      //freed id
      val freeId = new File(regionDir, "freeid")
      freeId.createNewFile()

      if (logger.isTraceEnabled())
        logger.trace(s"created region #$regionId at: $regionDir")
      new Region(isReplica(regionId), regionId, RegionConfig(regionDir, globalConfig))
    }

    regions += (regionId -> region)
    region
  }
}