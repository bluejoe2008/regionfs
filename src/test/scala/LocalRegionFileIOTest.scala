import java.io._

import cn.graiph.regionfs.util.Profiler._
import cn.graiph.regionfs.{BytePageOutput, GlobalConfig, Region, RegionConfig}
import org.apache.commons.io.IOUtils
import org.junit.{Assert, Before, Test}

/**
  * Created by bluejoe on 2020/2/11.
  */
class LocalRegionFileIOTest extends FileTestBase {
  @Before
  def makeFiles(): Unit = {
    makeFile(new File("./testdata/inputs/999"), 999L)
    makeFile(new File("./testdata/inputs/9999999"), 9999999L)
  }

  @Test
  def testRegionIO(): Unit = {
    val region = new Region(false, 131072,
      RegionConfig(new File("./testdata/nodes/node1/131072"),
        new GlobalConfig(1, -1, false)));

    val id = timing(true) {
      region.write(() => {
        new FileInputStream(new File("./testdata/inputs/9999999"))
      })
    }

    val baos = new ByteArrayOutputStream()
    val out = null;
    /*
    new BytePageOutput() {
      override def write(bytes: Array[Byte], offset: Int, length: Int): Unit = {
        baos.write(bytes, offset, length)
      }

      override def writeEOF(): Unit = {

      }
    }
    */

    val bytes = region.read(id, out)
    Assert.assertArrayEquals(baos.toByteArray,
      IOUtils.toByteArray(new FileInputStream(new File("./testdata/inputs/9999999"))));
  }
}
