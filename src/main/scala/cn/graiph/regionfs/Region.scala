package cn.graiph.regionfs

import java.io._
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.{CRC32, CheckedInputStream}

import cn.graiph.regionfs.util.Logging

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._


/**
  * Created by bluejoe on 2019/8/30.
  */
case class RegionConfig(regionDir: File) {

}

/**
  * metadata of a region
  */
case class MetaData(localId: Long, offset: Long, length: Long, crc32: Long) {

}

class RegionMetaFile(conf: RegionConfig) {
  val METADATA_ENTRY_LENGTH_WITH_PADDING = 40
  val fileMetaFile = new File(conf.regionDir, "meta")
  val writer = new RandomAccessFile(fileMetaFile, "rw");
  val reader = new RandomAccessFile(fileMetaFile, "r");

  //local id as offset
  def read(localId: Long): MetaData = {
    reader.seek(METADATA_ENTRY_LENGTH_WITH_PADDING * localId)
    val block = new Array[Byte](METADATA_ENTRY_LENGTH_WITH_PADDING);
    reader.readFully(block)

    val dis = new DataInputStream(new ByteArrayInputStream(block))
    val info = MetaData(dis.readLong(), dis.readLong(), dis.readLong(), dis.readLong())

    dis.close()
    info
  }

  def write(localId: Long, offset: Long, length: Long, crc32: Long): Unit = {
    //[iiii][iiii][oooo][oooo][llll][llll][cccc][cccc]
    val block = new ByteArrayOutputStream()
    val dos = new DataOutputStream(block)
    dos.writeLong(localId)
    dos.writeLong(offset)
    dos.writeLong(length)
    dos.writeLong(crc32)
    dos.writeLong(-1) //reserved
    writer.seek(METADATA_ENTRY_LENGTH_WITH_PADDING * localId)
    writer.write(block.toByteArray)
    dos.close()
  }

  def count = fileMetaFile.length() / METADATA_ENTRY_LENGTH_WITH_PADDING;

  def close(): Unit = {
    reader.close()
    writer.close()
  }
}

class FreeIdFile(conf: RegionConfig) {
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
          case _ => break
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
      try {
        consume(id)
        freeIds.remove(0)
        flush()
      }
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

class LocalIdGenerator(conf: RegionConfig, meta: RegionMetaFile) {
  //free id
  val counterLocalId = new AtomicLong(meta.count);
  val freeId = new FreeIdFile(conf)

  def consumeNextId(consume: (Long) => Unit): Long = {
    freeId.consumeNextId(consume).getOrElse {
      val id = counterLocalId.get();
      try {
        consume(id + 1)
        counterLocalId.incrementAndGet()
      }
    }
  }

  def close(): Unit = {
  }
}

class RegionBodyFile(conf: RegionConfig) {
  //TODO: too large
  val WRITE_BUFFER_SIZE = 10240
  //region file, one file for each region by far
  val fileBody = new File(conf.regionDir, "body")
  val fileBodyLength = new AtomicLong(fileBody.length())

  val reader = new RandomAccessFile(fileBody, "r");
  val appender = new FileOutputStream(fileBody, true)

  def write(source: () => InputStream): (Long, Long) = {
    //TODO: optimize writing
    val is = source()
    var n = 0
    var lengthWithPadding = 0L
    while (n >= 0) {
      //10K?
      val bytes = new Array[Byte](WRITE_BUFFER_SIZE)
      n = is.read(bytes)
      if (n > 0) {
        appender.write(bytes)
        lengthWithPadding += bytes.length
      }
    }

    appender.flush()
    (fileBodyLength.getAndAdd(lengthWithPadding), lengthWithPadding)
  }

  def close(): Unit = {
    appender.close()
    reader.close()
  }

  def read(offset: Int, length: Int): Array[Byte] = {
    reader.seek(offset)
    val content: Array[Byte] = new Array[Byte](length)
    reader.readFully(content)
    content
  }
}

/**
  * a Region store files in storeDir
  */
class Region(val replica: Boolean, val regionId: Long, conf: RegionConfig) extends Logging {
  //TODO: use ConfigServer
  val MAX_REGION_LENGTH = 102400

  //metadata file
  val fbody = new RegionBodyFile(conf)
  val fmeta = new RegionMetaFile(conf)
  val idgen = new LocalIdGenerator(conf, fmeta)

  def write(source: () => InputStream, length: Long, localId: Option[Long]): Long = {
    val (offset, written) = fbody.write(source)
    val crc32 = computeCrc32(source)

    //get local id
    idgen.consumeNextId((id: Long) => {
      fmeta.write(id, offset, length, crc32)
      logger.debug(s"[region-$regionId] written:localId=$localId, length=$length, occupied=$written")
    })
  }

  def close(): Unit = {
    fbody.close()
    fmeta.close()
    idgen.close()
  }

  def read(localId: Long, length: Long = -1): Array[Byte] = {
    val meta = fmeta.read(localId)
    if (length < 0) {
      fbody.read(meta.offset.toInt, meta.length.toInt)
    }
    else {
      fbody.read(meta.offset.toInt, length.toInt)
    }
  }

  def computeCrc32(getInputStream: () => InputStream): Long = {
    //get crc32
    val crc32 = new CRC32()
    val cis = new CheckedInputStream(getInputStream(), crc32)
    while (cis.read() != -1) {
    }
    val crc32Value = crc32.getValue
    cis.close()
    crc32Value
  }

  def length = fbody.fileBodyLength.get()
}

/**
  * RegionManager manages local regions stored in storeDir
  */
class RegionManager(nodeId: Long, storeDir: File) extends Logging {
  val regions = mutable.Map[Long, Region]()
  val regionIdSerial = new AtomicLong(0)

  def get(id: Long) = regions(id)

  private def isReplica(id: Long): Boolean = (id >> 16) != nodeId;
  /*
   layout of storeDir
    ./1
      body
      cursor
      meta
    ./2
      body
      cursor
      meta
    ...
  */
  regions ++= storeDir.listFiles().
    filter { file =>
      !file.isHidden && file.isDirectory
    }.
    map { file =>
      val id = file.getName.toLong
      id -> new Region(isReplica(id), id, RegionConfig(file))
    }

  logger.debug(s"loaded local regions: ${regions.keySet}")
  regionIdSerial.set((List(0L) ++ regions.map(_._1 >> 16).toList).max);

  def createNew() = {
    _createNewRegion(nodeId << 16 + regionIdSerial.incrementAndGet());
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

      logger.debug(s"created region #$regionId at: $regionDir")
      new Region(isReplica(regionId), regionId, RegionConfig(regionDir))
    }

    regions += (regionId -> region)
    region
  }
}
