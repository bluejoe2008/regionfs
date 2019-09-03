import java.io.{ByteArrayInputStream, File, FileInputStream, FileOutputStream}

import cn.graiph.blobfs.FsClient
import org.junit.{Before, Test}

/**
  * Created by bluejoe on 2019/8/23.
  */
class FsClientTest {
  val client = new FsClient("localhost:2181,localhost:2182,localhost:2183")

  @Before
  def makeFiles(): Unit = {
    makeFile(new File("./testdata/inputs/999"), 999)
    makeFile(new File("./testdata/inputs/9999"), 9999)
    makeFile(new File("./testdata/inputs/99999"), 99999)
  }

  private def makeFile(dst: File, length: Int): Unit = {
    val fos = new FileOutputStream(dst)
    var n: Int = 0
    while (n < length) {
      val left = length - n
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
    writeMsg()
    writeFile(new File("./testdata/inputs/999"))
    writeFile(new File("./testdata/inputs/9999"))
    writeFile(new File("./testdata/inputs/99999"))
  }

  @Test
  def test2(): Unit = {
    writeMsgs()
    writeFiles(new File("./testdata/inputs/999"))
    writeFiles(new File("./testdata/inputs/9999"))
    writeFiles(new File("./testdata/inputs/99999"))
  }

  @Test
  def test3(): Unit = {
    for (i <- 0 to 1000) {
      writeMsgs()
      writeFiles(new File("./testdata/inputs/999"))
      writeFiles(new File("./testdata/inputs/9999"))
      writeFiles(new File("./testdata/inputs/99999"))
    }
  }

  def writeMsgs(): Unit = {
    val msg = "hello, world"
    val fids = client.writeFiles((0 to 10).map { _ =>
      new ByteArrayInputStream(msg.getBytes) -> msg.getBytes.length.asInstanceOf[Long]
    })

    println(fids.map(_.asHexString()))
  }

  def writeFiles(src: File): Unit = {
    val fids = client.writeFiles((0 to 10).map { _ =>
      new FileInputStream(src) -> src.length
    })

    println(fids.map(_.asHexString()))
  }

  def writeMsg(): Unit = {
    val msg = "hello, world"
    val fid = client.writeFile(
      new ByteArrayInputStream(msg.getBytes), msg.getBytes.length.asInstanceOf[Long]
    )

    println(fid)
  }

  def writeFile(src: File): Unit = {
    val fid = client.writeFile(
      new FileInputStream(src), src.length)

    println(fid)
  }
}
