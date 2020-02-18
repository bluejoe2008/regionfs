import java.io.{ByteArrayInputStream, File, FileInputStream, FileOutputStream}

import cn.regionfs.{FsClient, FileId}
import cn.regionfs.util.Profiler

/**
  * Created by bluejoe on 2019/8/23.
  */
class FileTestBase {
  Profiler.enableTiming = true

  //run 3 processes first:
  // StartSingleTestServer ./node1.conf
  // StartSingleTestServer ./node2.conf
  // StartSingleTestServer ./node3.conf
  val client = new FsClient("localhost:2181")

  def writeFiles(src: File, times: Int): Unit = {
    val fids = client.writeFiles((0 to times - 1).map { _ =>
      new FileInputStream(src) -> src.length
    })

    println(fids.map(_.asHexString()))
  }

  def writeFileClock(src: File): FileId = clock(writeFile(src))

  def writeFilesClock(src: File, times: Int): Unit = clock(writeFiles(src: File, times: Int))

  def writeFileClock(text: String): FileId = clock(writeFile(text))

  def writeFileClock(bytes: Array[Byte]): FileId = clock(writeFile(bytes))

  def writeFile(src: File): FileId = {
    val fid = client.writeFile(
      new FileInputStream(src), src.length)
    fid
  }

  def writeFile(text: String): FileId = {
    writeFile(text.getBytes)
  }

  def writeFile(bytes: Array[Byte]): FileId = {
    val fid = client.writeFile(
      new ByteArrayInputStream(bytes), bytes.length)
    fid
  }

  def clock[T](runnable: => T): T = {
    val t1 = System.currentTimeMillis()
    val r = runnable;
    val t2 = System.currentTimeMillis()

    println(s"time: ${t2 - t1}ms")
    r;
  }

  def makeFile(dst: File, length: Long): Unit = {
    val fos = new FileOutputStream(dst)
    var n: Long = 0
    while (n < length) {
      val left: Int = Math.min((length - n).toInt, 10240)
      fos.write((0 to left - 1).map(x => ('a' + x % 26).toByte).toArray)
      n += left
    }

    fos.close()
  }
}
