import java.io._

import cn.graiph.regionfs.util.Profiler._
import cn.graiph.regionfs.{GlobalConfig, Region, RegionConfig}
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
  def test(): Unit = {
    val file = timing(true) {
      new File("./testdata/inputs/9999999")
    }

    timing(true) {
      IOUtils.toByteArray(new FileInputStream(file));
    }

    val reader = timing(true) {
      new RandomAccessFile(file, "r");
    }
    timing(true) {
      reader.seek(100)
    };
    val bytes = timing(true) {
      new Array[Byte](1024);
    }
    timing(true) {
      reader.readFully(bytes)
    };
    val bytes2 = timing(true) {
      new Array[Byte](1024 * 1024);
    }
    timing(true) {
      reader.readFully(bytes2)
    };
    timing(true) {
      reader.close()
    }
  }

  @Test
  def testRegionIO(): Unit = {
    val region = new Region(false, 131072,
      RegionConfig(new File("./testdata/nodes/node1/131072"),
        new GlobalConfig(1, -1, false)));

    val id = timing(true) {
      region.write(() => {
        new FileInputStream(new File("./testdata/inputs/9999999"))
      }, None)
    }

    val bytes = region.read(id, (is: InputStream) => {
      IOUtils.toByteArray(is)
    })

    Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File("./testdata/inputs/9999999"))), bytes);

    for (i <- 0 to 5) {
      timing(true) {
        region.read(id, (is: InputStream) => {
          IOUtils.toByteArray(is)
        })
      }
    }
  }
}
