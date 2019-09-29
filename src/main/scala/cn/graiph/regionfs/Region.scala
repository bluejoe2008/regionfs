package cn.graiph.regionfs

import java.io._
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.{CRC32, CheckedInputStream}

import scala.collection.mutable
import scala.util.control._

import cn.graiph.regionfs.util.Logging


/**
  * Created by bluejoe on 2019/8/30.
  */
/**
  * a Region store files in storeDir
  */
class Region(storeDir: File, val regionId: Long) extends Logging {
  //TODO: use ConfigServer
  val MAX_REGION_LENGTH = 102400
  val WRITE_BUFFER_SIZE = 10240

  //region file
  val fileBody = new File(storeDir, "body")
  //metadata file
  val fileMeta = new File(storeDir, "meta")
  //fileCursor stores last used localId
  //FIXME: fileCursor is not necessary, use count of metadata entries?
  val fileCursor = new File(storeDir, "cursor")

  //counterOffset = length of region
  val counterOffset =
    if (!fileBody.exists()) {
      fileBody.createNewFile()
      new AtomicLong(0)
    }
    else {
      new AtomicLong(fileBody.length())
    }

  //localId = index of each blob
  val counterLocalId =
    if (fileCursor.exists()) {
      val dis = new DataInputStream(new FileInputStream(fileCursor))
      val id = dis.readInt()
      dis.close()
      new AtomicLong(id)
    }
    else {
      new AtomicLong(0)
    }

  val bodyPtr = new FileOutputStream(fileBody, true)
  val metaPtr = new DataOutputStream(new FileOutputStream(fileMeta, true))

  def save(getInputStream: () => InputStream, length: Long, localId: Option[Long]): Long = {
    val lengthWithPadding = writeFileBody(getInputStream)
    val crc32 = computeCrc32(getInputStream)

    //get local id
    val id = {
      if (localId.isDefined) {
        localId.get
        //TODO: sync local id
      }
      else {
        counterLocalId.get()
      }
    }

    writeMeta(id, counterOffset.getAndAdd(lengthWithPadding), length, crc32)
    updateCursor(counterLocalId.incrementAndGet())

    logger.debug(s"saved $length bytes in region #$regionId")
    id
  }

  def close(): Unit = {
    bodyPtr.close()
    metaPtr.close()
  }

  def read(localId: Long, length: Long = -1): Array[Byte] ={
    val fMeta = getMetaByLocalId(localId)
    if (length < 0){
      readFileBody(fMeta("offset").toInt, fMeta("length").toInt)
    }
    else{
      readFileBody(fMeta("offset").toInt, length.toInt)
    }
  }

  //TODO: write less times
  private def updateCursor(id: Long) {
    val dos = new DataOutputStream(new FileOutputStream(fileCursor))
    dos.writeLong(id)
    dos.close()
  }

  private def writeFileBody(getInputStream: () => InputStream): Long = {
    //TODO: optimize writing
    val is = getInputStream()
    var n = 0
    var lengthWithPadding = 0L
    while (n >= 0) {
      //10K?
      val bytes = new Array[Byte](WRITE_BUFFER_SIZE)
      n = is.read(bytes)
      if (n > 0) {
        bodyPtr.write(bytes)
        lengthWithPadding += bytes.length
      }
    }

    lengthWithPadding
  }

  private def readFileBody( offset: Int, length: Int): Array[Byte] = {
    val is = new FileInputStream(fileBody)
    val content: Array[Byte] = new Array[Byte](length)
    is.read(content,offset.toInt,length.toInt)
    content
  }

  private def writeMeta(localId: Long, offset: Long, length: Long, crc32: Long): Unit = {
    //[llll][llll][oooo][oooo][llll][llll][cccc][cccc]
    //TODO: add a byte: DELETED?
    metaPtr.writeLong(localId)
    metaPtr.writeLong(offset)
    metaPtr.writeLong(length)
    metaPtr.writeLong(crc32)
  }

  private def getMetaByLocalId(localId: Long): Map[String,Long] = {
    val is =  new DataInputStream(new FileInputStream(fileMeta))
    val info = mutable.Map[String,Long]()
    val loop = new Breaks
    loop.breakable{
      while(is.available()>0) {
        val f_id = is.readLong()
        println(f_id)
        val offset = is.readLong()
        val length = is.readLong()
        val crc32 = is.readLong()
        if(f_id == localId){
          info("localId") = f_id
          info("offset") = offset
          info("length") = length
          info("crc32") = crc32
          loop.break()
        }
      }
    }
    is.close()
    info.toMap
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
}

/**
  * RegionManager manages local regions stored in storeDir
  */
class RegionManager(storeDir: File) extends Logging {
  val regions = mutable.Map[Long, Region]()

  def get(id: Long) = regions(id)

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
      id -> new Region(file, file.getName.toLong)
    }

  logger.debug(s"loaded local regions: ${regions.keySet}")

  def createNew(regionId: Long) = {
    val regionDir = new File(storeDir, s"$regionId")
    regionDir.mkdir()
    val region = new Region(regionDir, regionId)
    regions += (regionId -> region)
    logger.debug(s"created region #$regionId at: $regionDir")

    region
  }
}
