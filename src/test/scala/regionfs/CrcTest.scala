package regionfs

import java.io.{BufferedInputStream, File, FileInputStream}
import java.nio.ByteBuffer

import org.grapheco.commons.util.Profiler
import org.grapheco.commons.util.Profiler._
import org.grapheco.regionfs.util.CrcUtils
import org.junit.Test

/**
  * Created by bluejoe on 2020/3/3.
  */
class CrcTest {
  Profiler.enableTiming = true

  @Test
  def test1(): Unit = {
    val c1 = timing(true) {
      CrcUtils.computeCrc32(new BufferedInputStream(
        new FileInputStream(new File(s"./testdata/inputs/999"))));
    }

    println(c1);

    val buf = timing(true) {
      ByteBuffer.allocate(9999999);
    }

    timing(true) {
      new FileInputStream(new File(s"./testdata/inputs/9999999")).getChannel.read(buf);
    }

    val c2 = timing(true) {
      buf.flip()
      CrcUtils.computeCrc32(buf);
    }

    println(c2);

    val c3 = timing(true) {
      CrcUtils.computeCrc32(new BufferedInputStream(
        new FileInputStream(new File(s"./testdata/inputs/9999999"))));
    }

    println(c3);
  }
}
