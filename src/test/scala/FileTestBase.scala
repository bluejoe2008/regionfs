import java.io.{FileOutputStream, ByteArrayInputStream, File, FileInputStream}

import cn.graiph.regionfs.{FileId, FsClient}

/**
  * Created by bluejoe on 2019/8/23.
  */
class FileTestBase {
  //run 3 processes first:
  // StartSingleTestServer ./node1.conf
  // StartSingleTestServer ./node2.conf
  // StartSingleTestServer ./node3.conf
  val client = new FsClient("localhost:2181")

  def writeFiles(src: File, times: Int): Unit = {
    clock {
      val fids = client.writeFiles((0 to times - 1).map { _ =>
        new FileInputStream(src) -> src.length
      })

      println(fids.map(_.asHexString()))
    }
  }

  def writeFile(src: File): FileId = {
    clock {
      val fid = client.writeFile(
        new FileInputStream(src), src.length)
      println(fid)
      fid
    }
  }

  def writeFile(text: String): FileId = {
    clock {
      val fid = client.writeFile(
        new ByteArrayInputStream(text.getBytes), text.getBytes.length)
      println(fid)
      fid
    }
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
    var i: Byte = 'a'
    while (n < length) {
      val left: Int = (length - n).toInt
      val bytes = new Array[Byte](if (left < 10240) {
        left
      } else {
        10240
      })

      fos.write(bytes.map(_ => i.toByte))
      n += bytes.length
      i = (i + 1).toByte
      if (i > 'z') {
        i = 'a'
      }
    }

    fos.close()
  }
}
