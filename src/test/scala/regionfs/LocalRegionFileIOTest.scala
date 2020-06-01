package regionfs

import java.io._
import java.nio.ByteBuffer

import org.apache.commons.io.IOUtils
import org.grapheco.commons.util.Profiler._
import org.grapheco.regionfs.GlobalSetting
import org.grapheco.regionfs.server.LocalRegionManager
import org.grapheco.regionfs.util.{Atomic, CrcUtils, TransactionRunner}
import org.junit.{Assert, Test}

/**
  * Created by bluejoe on 2020/2/11.
  */
class LocalRegionFileIOTest extends FileTestBase {
  @Test
  def testRegionIO(): Unit = {
    val rm = new LocalRegionManager(1, new File("./testdata/nodes/node1"),
      GlobalSetting.empty, nullRegionEventListener);

    val region = rm.createNew()

    Assert.assertEquals(65537, region.regionId)
    Assert.assertEquals(true, region.isPrimary)
    Assert.assertEquals(0, region.revision)
    Assert.assertEquals(0, region.bodyLength)

    val bytes1 = IOUtils.toByteArray(new FileInputStream(new File("./testdata/inputs/9999999")))
    val buf = ByteBuffer.wrap(bytes1)

    val id = timing(true, 10) {
      val clone = buf.duplicate()
      val tx = Atomic("create local id") {
        case _ =>
          region.createLocalId()
      } --> Atomic("save local file") {
        case localId: Long =>
          region.stageFile(localId, clone, CrcUtils.computeCrc32(buf.duplicate()))
      } --> Atomic("mark global written") {
        case localId: Long =>
          region.commitFile(localId)
      }

      TransactionRunner.perform(tx, 1).asInstanceOf[Long]
    }

    val bytes2 = timing(true, 10) {
      val buf = region.read(id).get
      val bytes = new Array[Byte](buf.remaining())
      buf.get(bytes)
      bytes
    }

    Assert.assertArrayEquals(bytes1, bytes2);
  }
}
