package regionfs

import java.io.{File, FileInputStream}
import java.nio.ByteBuffer

import org.apache.commons.io.IOUtils
import org.grapheco.commons.util.Profiler._
import org.grapheco.regionfs.client.FsNodeClient
import org.grapheco.regionfs.util.ByteBufferConversions._
import org.junit.{After, Assert, Before, Test}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Created by bluejoe on 2019/8/23.
  */
class FsNodeClientTest extends FileTestBase {

  var nodeClient: FsNodeClient = null

  @Before
  def setup2(): Unit = {
    nodeClient = client.clientFactory.of("localhost:1224")
  }

  @After
  def after2() = {
  }

  @Test
  def testWrite(): Unit = {

    timing(true) {
      super.writeFile("hello, world")
    }

    for (i <- Array(999, 9999, 99999, 999999, 9999999)) {
      println(s"writing $i bytes...")
      val id = timing(true, 10) {
        Await.result(nodeClient.writeFile(new File(s"./testdata/inputs/$i")), Duration.Inf);
      }
    }
  }

  @Test
  def testWriteAsync(): Unit = {
    timing(true) {
      (1 to 10).map(_ => nodeClient.writeFile(
        ByteBuffer.wrap("hello, world".getBytes()))).map(Await.result(_, Duration.Inf))
    }

    for (i <- Array(999, 9999, 99999, 999999, 9999999)) {
      println(s"writing $i bytes...")
      timing(true) {
        (1 to 10).map(_ => nodeClient.writeFile(
          new File(s"./testdata/inputs/$i")
        )).map(Await.result(_, Duration.Inf))
      }
    }
  }

  @Test
  def testRead(): Unit = {
    for (i <- Array(999, 9999, 99999, 999999, 9999999)) {
      val src: File = new File(s"./testdata/inputs/$i")
      val id = Await.result(nodeClient.writeFile(src), Duration.Inf);

      println("=================================")
      println(s"file size: ${src.length()}");

      println("read a local file...")
      val bytes1 = timing(true) {
        IOUtils.toByteArray(new FileInputStream(src));
      }

      println("read an remote file...")
      val bytes2 = timing(true, 10) {
        IOUtils.toByteArray(nodeClient.readFile(id, Duration("4s")))
      };

      Assert.assertArrayEquals(bytes1, bytes2)
      println("=================================")
    }
  }
}
