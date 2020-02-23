package regionfs

import java.io._

import cn.regionfs.GlobalConfig
import cn.regionfs.server.{Region, RegionConfig}
import cn.bluejoe.util.Profiler._
import io.netty.buffer.{ByteBufInputStream, Unpooled}
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

    val id = timing(true, 10) {
      region.write(() => {
        new FileInputStream(new File("./testdata/inputs/9999999"))
      })
    }

    val bytes = timing(true, 10) {
      val buf = Unpooled.buffer(1024);
      region.writeTo(id, buf)
      IOUtils.toByteArray(new ByteBufInputStream(buf))
    }
    Assert.assertArrayEquals(
      IOUtils.toByteArray(new FileInputStream(new File("./testdata/inputs/9999999"))),
      bytes);
  }
}
