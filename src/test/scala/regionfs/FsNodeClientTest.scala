package regionfs

import java.io.{File, FileInputStream}
import java.nio.ByteBuffer

import io.netty.buffer.Unpooled
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
    implicit val ec = client1.executionContext
    val future = client1.createSecondaryFiles(
      65537,
      Array(1),
      System.currentTimeMillis(),
      Array(
        Tuple3(999L,
          CrcUtils.computeCrc32(new FileInputStream(new File(s"./testdata/inputs/999"))),
          Unpooled.wrappedBuffer(IOUtils.toByteArray(new FileInputStream(new File(s"./testdata/inputs/999"))))
        )
      )
    )
    val res = Await.result(future.map(x => FileId(x.regionId, x.localIds.head)), Duration.Inf)
    Assert.assertEquals(FileId(65537, 1), res)
  }

  @Test
  def testAsync(): Unit = {
    //create region-65537
    client.writeFile(ByteBuffer.wrap(IOUtils.toByteArray(new FileInputStream(new File(s"./testdata/inputs/999")))))
    val client1 = client.clientOf(1)
    val futures = (1 to 10).map(x =>
      client1.createSecondaryFiles(
        65537,
        Array(x),
        System.currentTimeMillis(),
        Array(
          Tuple3(999L,
            CrcUtils.computeCrc32(new FileInputStream(new File(s"./testdata/inputs/999"))),
            Unpooled.wrappedBuffer(IOUtils.toByteArray(new FileInputStream(new File(s"./testdata/inputs/999"))))
          )
        )
      )
    )
    implicit val ec = client1.executionContext
    val res = futures.map(future => Await.result(future.map(x => FileId(x.regionId, x.localIds.head)), Duration.Inf))
    Assert.assertEquals(FileId(65537, 1), res.head)
    Assert.assertEquals(FileId(65537, 10), res.takeRight(1).apply(0))
  }
}
