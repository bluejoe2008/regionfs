package regionfs

import java.io.{File, FileInputStream}

import cn.bluejoe.util.Profiler._
import org.apache.commons.io.IOUtils
import org.junit.{Assert, Before, Test}

/**
  * Created by bluejoe on 2019/8/23.
  */
class FileReadWriteTest extends FileTestBase {

  @Before
  def makeFiles(): Unit = {
    makeFile(new File("./testdata/inputs/999"), 999)
    makeFile(new File("./testdata/inputs/9999"), 9999)
    makeFile(new File("./testdata/inputs/99999"), 99999L)
    makeFile(new File("./testdata/inputs/999999"), 999999L)
    makeFile(new File("./testdata/inputs/9999999"), 9999999L)
  }

  @Test
  def testWrite(): Unit = {
    timing(true) {
      super.writeFile("hello, world")
    }

    for (i <- Array(999, 9999, 99999, 999999, 9999999)) {
      println(s"writing $i bytes...")
      timing(true, 10) {
        super.writeFile(new File(s"./testdata/inputs/$i"))
      }
    }
  }

  @Test
  def testRead(): Unit = {
    for (i <- Array(999, 9999, 99999, 999999, 9999999)) {
      val src: File = new File(s"./testdata/inputs/$i")
      val id = super.writeFile(src);

      println("=================================")
      println(s"file size: ${src.length()}");

      println("read a local file...")
      val bytes1 = timing(true) {
        IOUtils.toByteArray(new FileInputStream(src));
      }

      println("read an remote file...")
      val bytes2 = timing(true, 10) {
        IOUtils.toByteArray(client.readFile(id))
      };

      Assert.assertArrayEquals(bytes1, bytes2)
      println("=================================")
    }
  }
}
