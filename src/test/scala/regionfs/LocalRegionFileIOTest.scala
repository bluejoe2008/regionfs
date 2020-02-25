package regionfs

import java.io._
import java.nio.ByteBuffer

import cn.bluejoe.regionfs.GlobalConfig
import cn.bluejoe.regionfs.server.{Region, RegionConfig}
import cn.bluejoe.util.Profiler._
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

    val bytes1 = IOUtils.toByteArray(new FileInputStream(new File("./testdata/inputs/9999999")))
    val buf = ByteBuffer.wrap(bytes1)

    val id = timing(true, 10) {
      val clone = buf.duplicate()
      region.write(clone)
    }

    val bytes2 = timing(true, 10) {
      val buf = region.read(id)
      val bytes = new Array[Byte](buf.remaining())
      buf.get(bytes)
      bytes
    }

    Assert.assertArrayEquals(bytes1, bytes2);
  }
}
