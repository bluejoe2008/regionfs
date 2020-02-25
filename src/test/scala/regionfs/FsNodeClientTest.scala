package regionfs

import java.io.{ByteArrayInputStream, File, FileInputStream}

import cn.bluejoe.regionfs.GlobalConfig
import cn.bluejoe.regionfs.client.FsNodeClient
import cn.bluejoe.regionfs.server.RegionManager
import cn.bluejoe.util.Profiler._
import org.apache.commons.io.IOUtils
import org.junit.{Assert, Before, Test}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Created by bluejoe on 2019/8/23.
  */
class FsNodeClientTest extends FileTestBase {

  val nodeClient = FsNodeClient.connect("localhost:1224")
  val rm = new RegionManager(1, new File("./testdata/nodes/node1"), GlobalConfig(1, -1, false));

  @Before
  def makeFiles(): Unit = {
    makeFile(new File("./testdata/inputs/999"), 999)
    makeFile(new File("./testdata/inputs/9999"), 9999)
    makeFile(new File("./testdata/inputs/99999"), 99999L)
    makeFile(new File("./testdata/inputs/999999"), 999999L)
    makeFile(new File("./testdata/inputs/9999999"), 9999999L)
  }

  @Test
  def testWrite(): Unit = {
    timing(true) {
      super.writeFile("hello, world")
    }

    for (i <- Array(999, 9999, 99999, 999999, 9999999)) {
      println(s"writing $i bytes...")
      val id = timing(true, 10) {
        Await.result(nodeClient.writeFile(new FileInputStream(new File(s"./testdata/inputs/$i")), i), Duration.Inf);
      }

      //read local region
      val region = rm.get(id.regionId)
      val buf = region.read(id.localId)
      val bytes = new Array[Byte](buf.remaining())
      buf.get(bytes)
      Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File(s"./testdata/inputs/$i"))), bytes)
    }
  }

  @Test
  def testWriteAsync(): Unit = {
    timing(true) {
      (1 to 10).map(_ => nodeClient.writeFile(
        new ByteArrayInputStream("hello, world".getBytes()), "hello, world".getBytes().length.toInt
      )).map(Await.result(_, Duration.Inf))
    }

    for (i <- Array(999, 9999, 99999, 999999, 9999999)) {
      println(s"writing $i bytes...")
      timing(true) {
        (1 to 10).map(_ => nodeClient.writeFile(
          new FileInputStream(new File(s"./testdata/inputs/$i")), i
        )).map(Await.result(_, Duration.Inf))
      }
    }
  }

  @Test
  def testRead(): Unit = {
    for (i <- Array(999, 9999, 99999, 999999, 9999999)) {
      val src: File = new File(s"./testdata/inputs/$i")
      val id = Await.result(nodeClient.writeFile(new FileInputStream(src), i), Duration.Inf);

      println("=================================")
      println(s"file size: ${src.length()}");

      println("read a local file...")
      val bytes1 = timing(true) {
        IOUtils.toByteArray(new FileInputStream(src));
      }

      println("read an remote file...")
      val bytes2 = timing(true, 10) {
        IOUtils.toByteArray(nodeClient.readFile(id))
      };

      Assert.assertArrayEquals(bytes1, bytes2)
      println("=================================")
    }
  }
}
