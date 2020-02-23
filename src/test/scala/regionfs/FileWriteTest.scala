package regionfs

import java.io.File

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
    writeFileClock(new File("./testdata/inputs/999"))
    writeFileClock(new File("./testdata/inputs/9999"))
    writeFileClock(new File("./testdata/inputs/99999"))
    writeFileClock(new File("./testdata/inputs/999999"))
  }

  @Test
  def test0(): Unit = {
    writeFileClock("this is a test")
    writeFileClock("hello, world")
  }

  @Test
  def test2(): Unit = {
    writeFilesClock(new File("./testdata/inputs/999"), 2)
    writeFilesClock(new File("./testdata/inputs/9999"), 2)
    writeFilesClock(new File("./testdata/inputs/99999"), 2)
    writeFilesClock(new File("./testdata/inputs/999999"), 2)
  }

  @Test
  def test3(): Unit = {
    writeFilesClock(new File("./testdata/inputs/999"), 3)
    writeFilesClock(new File("./testdata/inputs/9999"), 3)
    writeFilesClock(new File("./testdata/inputs/99999"), 3)
    writeFilesClock(new File("./testdata/inputs/999999"), 3)
  }

  @Test
  def test4(): Unit = {
    writeFilesClock(new File("./testdata/inputs/999"), 4)
    writeFilesClock(new File("./testdata/inputs/9999"), 4)
    writeFilesClock(new File("./testdata/inputs/99999"), 4)
    writeFilesClock(new File("./testdata/inputs/999999"), 4)
  }

  @Test
  def test10(): Unit = {
    writeFilesClock(new File("./testdata/inputs/999"), 10)
    writeFilesClock(new File("./testdata/inputs/9999"), 10)
    writeFilesClock(new File("./testdata/inputs/99999"), 10)
    writeFilesClock(new File("./testdata/inputs/999999"), 10)
  }
}
