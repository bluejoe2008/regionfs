package regionfs

import java.io.File

import cn.bluejoe.util.Profiler._
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
  def test(): Unit = {
    timing(true) {
      super.writeFile("hello, world")
    }
    timing(true, 10) {
      super.writeFile(new File("./testdata/inputs/999"))
    }
    timing(true, 10) {
      super.writeFile(new File("./testdata/inputs/9999"))
    }
    timing(true, 10) {
      super.writeFile(new File("./testdata/inputs/99999"))
    }
    timing(true, 10) {
      super.writeFile(new File("./testdata/inputs/999999"))
    }
  }
}
