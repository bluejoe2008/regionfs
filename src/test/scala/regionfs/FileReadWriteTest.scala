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
class FileReadWriteTest extends FileTestBase {

  @Test
  def testWrite(): Unit = {
    timing(true) {
      super.writeFile("hello, world")
    }

    for (i <- BLOB_LENGTH) {
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

    for (i <- BLOB_LENGTH) {
      println(s"writing $i bytes...")
      val ids = timing(true) {
        (1 to 10).map(_ => super.writeFileAsync(new File(s"./testdata/inputs/$i"))).map(Await.result(_, Duration.Inf))
      }
    }
  }

  @Test
  def testReadAsync(): Unit = {
    timing(true) {
      (1 to 10).map(_ => super.writeFileAsync("hello, world")).map(Await.result(_, Duration.Inf))
    }

    for (i <- BLOB_LENGTH) {
      val id = super.writeFile(new File(s"./testdata/inputs/$i"))

      println(s"reading $i bytes...")
      timing(true) {
        val futures = (1 to 10).map(x =>
          Future {
            IOUtils.toByteArray(client.readFile(id, Duration("4s")))
          }
        )

        futures.foreach(Await.result(_, Duration("4s")))
      }
    }
  }

  @Test
  def testDelete(): Unit = {
    val src: File = new File(s"./testdata/inputs/999")
    val id = super.writeFile(src);
    IOUtils.toByteArray(client.readFile(id, Duration("4s")))

    timing(true) {
      Await.result(client.deleteFile(id), Duration.Inf)
    }

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
