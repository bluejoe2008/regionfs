import java.io.{File, FileInputStream}

import org.apache.commons.io.IOUtils
import org.junit.{Assert, Before, Test}

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
      Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(src)), bytes);
      println(s"size: ${bytes.size}");
    }

    println("read an remote file...")
    for (i <- 0 to 5) {
      clock {
        client.readFile(id);
      }
    }

    println("read a local file...")
    clock {
      IOUtils.toByteArray(new FileInputStream(src));
    }
  }
}
