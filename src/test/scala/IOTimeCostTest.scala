import java.io._
import java.nio.ByteBuffer

import cn.regionfs.util.Profiler._
import cn.regionfs.util.{Profiler, StreamUtils}
import org.apache.commons.io.IOUtils
import org.junit.Test

/**
  * Created by bluejoe on 2020/2/13.
  */
class IOTimeCostTest {
  Profiler.enableTiming = true

  @Test
  def testSerialize(): Unit = {
    val bytes = new Array[Byte](1024 * 1024);
    timing(true) {
      bytes.toArray
    }
    val bs2 = timing(true, 10) {
      StreamUtils.serializeObject(bytes)
    }

    println(bs2.length)
  }

  @Test
  def testByteBuffer(): Unit = {
    val bytes = (0 to 31).map(_.toByte).toArray

    timing(true) {
      val dis = new DataInputStream(new ByteArrayInputStream(bytes))
      println(dis.readLong(), dis.readLong())

      dis.close()
    }

    timing(true) {
      val buffer = ByteBuffer.wrap(bytes)
      println(buffer.getLong, buffer.getLong)
    }
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
}
