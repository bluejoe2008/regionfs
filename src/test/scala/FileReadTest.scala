import java.io.{File, FileInputStream}

import org.apache.commons.io.IOUtils
import org.junit.{Before, Assert, Test}

/**
  * Created by bluejoe on 2020/1/8.
  */
class FileReadTest extends FileTestBase {
  @Before
  def makeFiles(): Unit = {
    makeFile(new File("./testdata/inputs/999"), 999)
    makeFile(new File("./testdata/inputs/9999"), 9999)
    makeFile(new File("./testdata/inputs/99999"), 99999L)
    makeFile(new File("./testdata/inputs/999999"), 999999L)
    makeFile(new File("./testdata/inputs/9999999"), 9999999L)
  }

  @Test
  def test(): Unit = {
    val src: File = new File("./testdata/inputs/9999999")
    val id = super.writeFile(src);
    clock {
      val bytes = client.readFile(id);
      Assert.assertArrayEquals(bytes, IOUtils.toByteArray(new FileInputStream(src)));
    }
    clock {
      val bytes = client.readFile(id);
    }
    clock {
      println("read an remote file...")
      val bytes = client.readFile(id);
      println(s"size: ${bytes.size}");
    }
    clock {
      println("read a local file...")
      val bytes = IOUtils.toByteArray(new FileInputStream(src));
      println(s"size: ${bytes.size}");
    }
  }
}
