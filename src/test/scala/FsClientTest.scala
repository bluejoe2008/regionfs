import java.io.{ByteArrayInputStream, File, FileInputStream}

import cn.graiph.blobfs.FsClient
import cn.graiph.blobfs.shell.StartNodeServer
import org.apache.commons.io.FileUtils
import org.junit.{BeforeClass, Before, Test}

/**
  * Created by bluejoe on 2019/8/23.
  */
class FsClientTest {
  val client = new FsClient("localhost:2181,localhost:2182,localhost:2183");

  @Test
  def test(): Unit = {
    val src = new File("/Users/bluejoe/derby.log");
    for (i <- 0 to 10000) {
      writeMsg();
      writeFile(src);
    }
  }

  def writeMsg(): Unit = {
    val msg = "hello, world";
    val fid = client.writeFile(
      new ByteArrayInputStream(msg.getBytes), msg.getBytes.length);
    println(fid.asHexString());
  }

  def writeFile(src: File): Unit = {
    val fid = client.writeFile(new FileInputStream(src), src.length);
    println(fid.asHexString());
  }
}
