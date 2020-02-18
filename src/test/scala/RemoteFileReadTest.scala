import java.io.{File, FileInputStream}

import org.apache.commons.io.IOUtils
import org.junit.{Assert, Before, Test}
import cn.regionfs.util.Profiler._

/**
  * Created by bluejoe on 2020/1/8.
  */
class RemoteFileReadTest extends FileTestBase {
  @Before
  def makeFiles(): Unit = {
    makeFile(new File("./testdata/inputs/999"), 999)
    makeFile(new File("./testdata/inputs/9999"), 9999)
    makeFile(new File("./testdata/inputs/99999"), 99999L)
    makeFile(new File("./testdata/inputs/999999"), 999999L)
    makeFile(new File("./testdata/inputs/9999999"), 9999999L)
    makeFile(new File("./testdata/inputs/99999999"), 99999999L)
  }

  @Test
  def test999(): Unit = {
    test("./testdata/inputs/999");
  }

  @Test
  def test9999(): Unit = {
    test("./testdata/inputs/9999");
  }

  @Test
  def test99999(): Unit = {
    test("./testdata/inputs/99999");
  }

  @Test
  def test999999(): Unit = {
    test("./testdata/inputs/999999");
  }

  @Test
  def test9999999(): Unit = {
    test("./testdata/inputs/9999999");
  }

  private def test(path: String): Unit = {
    val src: File = new File(path)
    val id = super.writeFile(src);

    println("read an remote file via readFile()...")
    Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(src)), timing(true) {
      IOUtils.toByteArray(client.readFile(id))
    });

    println("read a local file...")
    val bytes2 = timing(true) {
      IOUtils.toByteArray(new FileInputStream(src));
    }

    println(s"size: ${bytes2.size}");
  }
}
