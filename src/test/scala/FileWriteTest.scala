import java.io.{File, FileOutputStream}

import org.junit.{Before, Test}

/**
  * Created by bluejoe on 2019/8/23.
  */
class FileWriteTest extends FileTestBase {

  @Before
  def makeFiles(): Unit = {
    makeFile(new File("./testdata/inputs/999"), 999)
    makeFile(new File("./testdata/inputs/9999"), 9999)
    makeFile(new File("./testdata/inputs/99999"), 99999L)
    makeFile(new File("./testdata/inputs/999999"), 999999L)
  }

  @Test
  def test1(): Unit = {
    writeFile(new File("./testdata/inputs/999"))
    writeFile(new File("./testdata/inputs/9999"))
    writeFile(new File("./testdata/inputs/99999"))
    writeFile(new File("./testdata/inputs/999999"))
  }

  @Test
  def test0(): Unit = {
    writeFile("this is a test")
    writeFile("hello, world")
  }

  @Test
  def test2(): Unit = {
    writeFiles(new File("./testdata/inputs/999"), 2)
    writeFiles(new File("./testdata/inputs/9999"), 2)
    writeFiles(new File("./testdata/inputs/99999"), 2)
    writeFiles(new File("./testdata/inputs/999999"), 2)
  }

  @Test
  def test3(): Unit = {
    writeFiles(new File("./testdata/inputs/999"), 3)
    writeFiles(new File("./testdata/inputs/9999"), 3)
    writeFiles(new File("./testdata/inputs/99999"), 3)
    writeFiles(new File("./testdata/inputs/999999"), 3)
  }

  @Test
  def test4(): Unit = {
    writeFiles(new File("./testdata/inputs/999"), 4)
    writeFiles(new File("./testdata/inputs/9999"), 4)
    writeFiles(new File("./testdata/inputs/99999"), 4)
    writeFiles(new File("./testdata/inputs/999999"), 4)
  }

  @Test
  def test10(): Unit = {
    writeFiles(new File("./testdata/inputs/999"), 10)
    writeFiles(new File("./testdata/inputs/9999"), 10)
    writeFiles(new File("./testdata/inputs/99999"), 10)
    writeFiles(new File("./testdata/inputs/999999"), 10)
  }
}
