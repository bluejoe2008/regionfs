import java.io.{File, FileInputStream, FileOutputStream}

import cn.graiph.blobfs.FsClient
import org.junit.{Before, Test}

/**
  * Created by bluejoe on 2019/8/23.
  */
class FsClientTest {
  //run 3 processes first:
  // StartSingleTestServer ./node1.conf
  // StartSingleTestServer ./node2.conf
  // StartSingleTestServer ./node3.conf
  val client = new FsClient("localhost:2181,localhost:2182,localhost:2183")

  @Before
  def makeFiles(): Unit = {
    makeFile(new File("./testdata/inputs/999"), 999)
    makeFile(new File("./testdata/inputs/9999"), 9999)
    makeFile(new File("./testdata/inputs/99999"), 99999L)
  }

  private def makeFile(dst: File, length: Long): Unit = {
    val fos = new FileOutputStream(dst)
    var n: Long = 0
    while (n < length) {
      val left: Int = (length - n).toInt
      val bytes = new Array[Byte](if (left < 10240) {
        left
      } else {
        10240
      })
      fos.write(bytes)
      n += bytes.length
    }

    fos.close()
  }

  @Test
  def test1(): Unit = {
    writeFile(new File("./testdata/inputs/999"))
    writeFile(new File("./testdata/inputs/9999"))
    writeFile(new File("./testdata/inputs/99999"))
  }

  @Test
  def test2(): Unit = {
    writeFiles(new File("./testdata/inputs/999"), 2)
    writeFiles(new File("./testdata/inputs/9999"), 2)
    writeFiles(new File("./testdata/inputs/99999"), 2)
  }

  @Test
  def test3(): Unit = {
    writeFiles(new File("./testdata/inputs/999"), 3)
    writeFiles(new File("./testdata/inputs/9999"), 3)
    writeFiles(new File("./testdata/inputs/99999"), 3)
  }

  @Test
  def test4(): Unit = {
    writeFiles(new File("./testdata/inputs/999"), 4)
    writeFiles(new File("./testdata/inputs/9999"), 4)
    writeFiles(new File("./testdata/inputs/99999"), 4)
  }

  @Test
  def test10(): Unit = {
    writeFiles(new File("./testdata/inputs/999"), 10)
    writeFiles(new File("./testdata/inputs/9999"), 10)
    writeFiles(new File("./testdata/inputs/99999"), 10)
  }

  def writeFiles(src: File, times: Int): Unit = {
    clock {
      val fids = client.writeFiles((0 to times - 1).map { _ =>
        new FileInputStream(src) -> src.length
      })

      println(fids.map(_.asHexString()))
    }
  }

  def writeFile(src: File): Unit = {
    clock {
      val fid = client.writeFile(
        new FileInputStream(src), src.length)
      println(fid)
    }
  }

  def clock(runnable: => Unit) {
    val t1 = System.currentTimeMillis()
    runnable;
    val t2 = System.currentTimeMillis()

    println(s"time: ${t2 - t1}ms")
  }
}
