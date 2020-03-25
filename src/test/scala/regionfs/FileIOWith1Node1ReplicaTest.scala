package regionfs

import java.io.{File, FileInputStream}

import org.apache.commons.io.IOUtils
import org.grapheco.commons.util.Profiler._
import org.junit.{Assert, Test}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * Created by bluejoe on 2019/8/23.
  */
class FileIOWith1Node1ReplicaTest extends FileTestBase {

  @Test
  def testWrite(): Unit = {
    val count1 = super.countFiles();

    timing(true) {
      super.writeFile("hello, world")
    }

    val count2 = super.countFiles();
    Assert.assertEquals(count2, count1 + 1)

    for (i <- BLOB_LENGTH) {
      println(s"writing $i bytes...")
      timing(true, 10) {
        super.writeFile(new File(s"./testdata/inputs/$i"))
      }
    }

    for (i <- BLOB_LENGTH) {
      val bytes = toBytes(new File(s"./testdata/inputs/$i"))
      for (m <- 1 to 10) {
        val id = super.writeFile(new File(s"./testdata/inputs/$i"))
        Assert.assertArrayEquals(bytes, IOUtils.toByteArray(client.readFile(id, Duration("4s"))))
      }
    }

    val count3 = super.countFiles();
    Assert.assertEquals(count3, count2 + 10 * BLOB_LENGTH.size + 10 * BLOB_LENGTH.size)
  }

  @Test
  def testWriteAsync(): Unit = {
    val count1 = super.countFiles();

    timing(true) {
      (1 to 10).map(_ => super.writeFileAsync("hello, world")).map(Await.result(_, Duration("4s")))
    }

    val count2 = super.countFiles();
    Assert.assertEquals(count2, count1 + 10)

    for (i <- BLOB_LENGTH) {
      println(s"writing $i bytes...")
      timing(true) {
        (1 to 10).map(_ => super.writeFileAsync(new File(s"./testdata/inputs/$i"))).map(Await.result(_, Duration("4s")))
      }
    }

    for (i <- BLOB_LENGTH) {
      val bytes = toBytes(new File(s"./testdata/inputs/$i"))

      val ids =
        (1 to 10).map(_ => super.writeFileAsync(new File(s"./testdata/inputs/$i"))).map(Await.result(_, Duration("4s")))

      for (id <- ids) {
        Assert.assertArrayEquals(bytes,
          IOUtils.toByteArray(client.readFile(id, Duration("4s"))))
      }
    }

    val count3 = super.countFiles();
    Assert.assertEquals(count3, count2 + 10 * BLOB_LENGTH.size + 10 * BLOB_LENGTH.size)
  }

  @Test
  def testReadAsync(): Unit = {
    for (i <- BLOB_LENGTH) {
      val id = super.writeFile(new File(s"./testdata/inputs/$i"))
      val bytes = toBytes(new File(s"./testdata/inputs/$i"))

      println(s"reading $i bytes...")
      timing(true) {
        val futures = (1 to 10).map(x =>
          Future {
            IOUtils.toByteArray(client.readFile(id, Duration("4s")))
          }
        )

        for (bf <- futures) {
          Assert.assertArrayEquals(bytes,
            Await.result(bf, Duration("4s")))
        }
      }
    }
  }

  @Test
  def testDelete(): Unit = {
    val count1 = super.countFiles();

    val src: File = new File(s"./testdata/inputs/999")
    val id = super.writeFile(src);
    IOUtils.toByteArray(client.readFile(id, Duration("4s")))

    val count2 = super.countFiles()
    Assert.assertEquals(count1 + 1, count2)

    timing(true) {
      Assert.assertEquals(true, Await.result(client.deleteFile(id), Duration.Inf))
    }

    val count3 = super.countFiles()
    Assert.assertEquals(count1, count3)

    timing(true) {
      Assert.assertEquals(false, Await.result(client.deleteFile(id), Duration.Inf))
    }

    val count4 = super.countFiles()
    Assert.assertEquals(count1, count4)

    try {
      IOUtils.toByteArray(client.readFile(id, Duration("4s")))
      Assert.assertTrue(false)
    }
    catch {
      case e: Throwable => {
        //e.printStackTrace()
        Assert.assertTrue(true)
      }
    }
  }

  @Test
  def testRead(): Unit = {
    for (i <- BLOB_LENGTH) {
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
