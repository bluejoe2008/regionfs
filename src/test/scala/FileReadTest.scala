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
    val bytes = IOUtils.toByteArray(client.readFile(id))

    Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(src)), bytes);

    println("read an remote file via readFile()...")
    clock {
      IOUtils.toByteArray(client.readFile(id))
    }

    println("read an remote file via readFile1()...")
    clock {
      IOUtils.toByteArray(client.readFile1(id))
    }

    println("read an remote file via readFile2()...")
    clock {
      IOUtils.toByteArray(client.readFile2(id))
    }

    println("read a local file...")
    val bytes2 = clock {
      IOUtils.toByteArray(new FileInputStream(src));
    }
    println(s"size: ${bytes2.size}");
  }
}
