import java.io.{ByteArrayInputStream, File, FileInputStream}

import cn.graiph.blobfs.FsNodeClient
import org.junit.Test

/**
  * Created by bluejoe on 2019/8/23.
  */
class FsNodeClientTest {
  @Test
  def test(): Unit = {
    val src = new File("/Users/bluejoe/derby.log");
    Array("localhost:1224", "localhost:1225").foreach((x: String) => {
      writeMsg(x);
      writeFile(src, x);
    })
  }

  def writeMsg(url: String): Unit = {
    val msg = "hello, world";
    val fid = FsNodeClient.connect(url).writeFile(
      new ByteArrayInputStream(msg.getBytes), msg.getBytes.length);
    println(fid.asHexString());
  }

  def writeFile(src: File, url: String): Unit = {
    val fid = FsNodeClient.connect(url).writeFile(new FileInputStream(src), src.length);
    println(fid.asHexString());
  }
}
