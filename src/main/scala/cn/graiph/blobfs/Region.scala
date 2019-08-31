package cn.graiph.blobfs

import java.io._
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.zip.{CRC32, CheckedInputStream}

import cn.graiph.blobfs.util.Logging

import scala.collection.mutable

/**
  * Created by bluejoe on 2019/8/30.
  */
class Region(storeDir: File, val regionId: Int) extends Logging {
  val MAX_REGION_LENGTH = 102400;
  val WRITE_BUFFER_SIZE = 10240;

  val fileBody = new File(storeDir, "body");
  val fileMeta = new File(storeDir, "meta");
  val fileCursor = new File(storeDir, "cursor");

  val counterOffset =
    if (!fileBody.exists()) {
      fileBody.createNewFile();
      new AtomicLong(0);
    }
    else {
      new AtomicLong(fileBody.length());
    }

  val counterLocalId =
    if (fileCursor.exists()) {
      val dis = new DataInputStream(new FileInputStream(fileCursor));
      val id = dis.readInt();
      dis.close();
      new AtomicInteger(id);
    }
    else {
      new AtomicInteger(0);
    }

  val bodyPtr = new FileOutputStream(fileBody, true);
  val metaPtr = new DataOutputStream(new FileOutputStream(fileMeta, true));

  def save(getInputStream: () => InputStream, length: Long, localId: Option[Int]): Int = {
    val lengthWithPadding = writeFileBody(getInputStream);
    val crc32 = computeCrc32(getInputStream);

    val id = {
      if (localId.isDefined) {
        localId.get
        //TODO: sync local id
      }
      else {
        counterLocalId.get()
      }
    };

    writeMeta(id, counterOffset.getAndAdd(lengthWithPadding), length, crc32);
    updateCursor(counterLocalId.incrementAndGet());

    logger.debug(s"saved $length bytes in region #$regionId");
    id;
  }

  def close(): Unit = {
    bodyPtr.close();
    metaPtr.close();
  }

  //TODO: write less
  private def updateCursor(id: Int) {
    val dos = new DataOutputStream(new FileOutputStream(fileCursor));
    dos.writeInt(id);
    dos.close();
  }

  private def writeFileBody(getInputStream: () => InputStream): Long = {
    //TOOD: optimize writing
    val is = getInputStream();
    var n = 0;
    var lengthWithPadding = 0L;
    while (n >= 0) {
      //10K?
      val bytes = new Array[Byte](WRITE_BUFFER_SIZE);
      n = is.read(bytes);
      if (n > 0) {
        bodyPtr.write(bytes);
        lengthWithPadding += bytes.length;
      }
    }

    lengthWithPadding
  }

  private def writeMeta(localId: Int, offset: Long, length: Long, crc32: Long): Unit = {
    //each entry uses 30bytes
    //[llll][oooo][oooo][llll][llll][cccc][cccc][__]
    metaPtr.writeInt(localId);
    metaPtr.writeLong(offset);
    metaPtr.writeLong(length);
    metaPtr.writeLong(crc32);
    metaPtr.write((0 to 1).map(_ => 0.toByte).toArray);
  }

  def computeCrc32(getInputStream: () => InputStream): Long = {
    //get crc32
    val crc32 = new CRC32();
    val cis = new CheckedInputStream(getInputStream(), crc32);
    while (cis.read() != -1) {
    }
    val crc32Value = crc32.getValue;
    cis.close();
    crc32Value
  }
}

class RegionManager(storeDir: File) extends Logging {
  val regions = mutable.Map[Int, Region]();

  def get(id: Int) = regions(id);

  regions ++= storeDir.listFiles().
    filter { file =>
      !file.isHidden && file.isDirectory
    }.
    map { file =>
      val id = file.getName.toInt;
      id -> new Region(file, file.getName.toInt);
    }

  logger.debug(s"loaded regions: ${regions.keySet}");

  def createNew(regionId: Int) = {
    val regionDir = new File(storeDir, s"$regionId");
    regionDir.mkdir();
    regions += (regionId -> new Region(regionDir, regionId));

    logger.debug(s"created region #$regionId at: $regionDir");
  }
}
