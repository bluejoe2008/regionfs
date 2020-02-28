package regionfs

import java.io.{File, FileInputStream}

import cn.bluejoe.util.Profiler._
import org.apache.commons.io.IOUtils
import org.junit.{Assert, Before, Test}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Created by bluejoe on 2019/8/23.
  */
class FileReadWriteTest extends FileTestBase {

  @Test
  def testWrite(): Unit = {
    timing(true) {
      super.writeFile("hello, world")
    }

    for (i <- Array(999, 9999, 99999, 999999, 9999999)) {
      println(s"writing $i bytes...")
      timing(true, 10) {
        super.writeFile(new File(s"./testdata/inputs/$i"))
      }
    }
  }

  @Test
  def testWriteAsync(): Unit = {
    timing(true) {
      (1 to 10).map(_ => super.writeFileAsync("hello, world")).map(Await.result(_, Duration.Inf))
    }

    for (i <- Array(999, 9999, 99999, 999999, 9999999)) {
      println(s"writing $i bytes...")
      timing(true) {
        (1 to 10).map(_ => super.writeFileAsync(new File(s"./testdata/inputs/$i"))).map(Await.result(_, Duration.Inf))
      }
    }
  }

  @Test
  def testRead(): Unit = {
    for (i <- Array(999, 9999, 99999, 999999, 9999999)) {
      val src: File = new File(s"./testdata/inputs/$i")
      val id = super.writeFile(src);

      println("=================================")
      println(s"file size: ${src.length()}");

      println("read a local file...")
      val bytes1 = timing(true) {
        IOUtils.toByteArray(new FileInputStream(src));
      }

      println("read an remote file...")
      val bytes2 = timing(true, 10) {
        IOUtils.toByteArray(client.readFile(id, Duration("4s")))
      };

      Assert.assertArrayEquals(bytes1, bytes2)
      println("=================================")
    }
  }
}
