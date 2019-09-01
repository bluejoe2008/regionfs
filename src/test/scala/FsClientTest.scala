import java.io.{ByteArrayInputStream, File, FileInputStream}

import cn.graiph.blobfs.FsClient
import org.junit.Test

/**
  * Created by bluejoe on 2019/8/23.
  */
class FsClientTest {
  val client = new FsClient("localhost:2181,localhost:2182,localhost:2183")

  @Test
  def test(): Unit = {
    for (i <- 0 to 10000) {
      writeMsgs()
      writeFiles(new File("/Users/bluejoe/Documents/neo4j-eclipse.xml"))
      writeFiles(new File("/Users/bluejoe/Documents/知识图谱的技术与应用.pdf"))
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
}
