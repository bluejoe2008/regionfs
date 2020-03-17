package regionfs

import java.io.{File, FileInputStream}
import java.nio.ByteBuffer

import org.apache.commons.io.IOUtils
import org.grapheco.regionfs.FileId
import org.grapheco.regionfs.util.CrcUtils
import org.junit.{Assert, Test}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Created by bluejoe on 2020/3/16.
  */
class FsNodeClientTest extends FileTestBase {
  @Test
  def testSync(): Unit = {
    //create region-65537
    client.writeFile(ByteBuffer.wrap(IOUtils.toByteArray(new FileInputStream(new File(s"./testdata/inputs/999")))))
    val client1 = client.clientOf(1)
    val future = client1.createSecondaryFile(65537, 1,
      999,
      CrcUtils.computeCrc32(new FileInputStream(new File(s"./testdata/inputs/999"))),
      ByteBuffer.wrap(IOUtils.toByteArray(new FileInputStream(new File(s"./testdata/inputs/999")))))
    val res = Await.result(future, Duration.Inf).fileId
    Assert.assertEquals(FileId(65537, 1), res)
  }

  @Test
  def testAsync(): Unit = {
    //create region-65537
    client.writeFile(ByteBuffer.wrap(IOUtils.toByteArray(new FileInputStream(new File(s"./testdata/inputs/999")))))
    val client1 = client.clientOf(1)
    val futures = (1 to 10).map(x => client1.createSecondaryFile(65537, 1,
      999,
      CrcUtils.computeCrc32(new FileInputStream(new File(s"./testdata/inputs/999"))),
      ByteBuffer.wrap(IOUtils.toByteArray(new FileInputStream(new File(s"./testdata/inputs/999"))))))
    val res = futures.map(Await.result(_, Duration.Inf).fileId)
    Assert.assertEquals(FileId(65537, 1), res.head)
    Assert.assertEquals(FileId(65537, 10), res.takeRight(1).apply(0))
  }
}
