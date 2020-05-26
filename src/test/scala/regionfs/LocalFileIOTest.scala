package regionfs

import java.io.{File, FileOutputStream, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.concurrent.Executors

import io.netty.buffer.Unpooled
import net.neoremind.kraps.util.ByteBufferInputStream
import org.apache.commons.io.{FileUtils, IOUtils}
import org.grapheco.commons.util.Profiler
import org.junit.{Before, Test}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * Created by bluejoe on 2020/3/17.
  */
class LocalFileIOTest {
  Profiler.enableTiming = true
  val fileBody = new File("./testdata/output/abc")

  @Before
  def setup() {
    FileUtils.deleteDirectory(fileBody.getParentFile)
    fileBody.getParentFile.mkdirs()
    fileBody.createNewFile()
  }

  @Test
  def testMultiFiles(): Unit = {
    //write all files serially
    val lens = (2014 to 3014)
    val fos = new FileOutputStream(fileBody, true)
    Profiler.timing() {
      for (len <- lens) {
        val bytes = new Array[Byte](len);
        fos.write(bytes)
        fos.flush()
      }
    }
    fos.close()

    //write all files parallelly, using scala.Future
    implicit val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(10))
    Profiler.timing() {
      val futures = lens.map { len =>
        Future {
          val fos = new FileOutputStream(new File(fileBody.getParent, len.toString))
          val bytes = new Array[Byte](len);
          fos.write(bytes)
          fos.close()
        }
      }
      futures.foreach(Await.result(_, Duration("4s")))
    }

    //write all files parallelly, using java.Future
    val executors = Executors.newFixedThreadPool(10)
    Profiler.timing() {
      val futures = lens.map { len =>
        executors.submit(new Runnable() {
          def run(): Unit = {
            val fos = new FileOutputStream(new File(fileBody.getParent, len.toString))
            val bytes = new Array[Byte](len);
            fos.write(bytes)
            fos.close()
          }
        })
      }

      futures.foreach(_.get())
    }

    //write all files parallelly, using new Thread(...)
    Profiler.timing() {
      val threads = lens.map { len =>
        new Thread(new Runnable {
          override def run(): Unit = {
            val fos = new FileOutputStream(new File(fileBody.getParent, len.toString))
            val bytes = new Array[Byte](len);
            fos.write(bytes)
            fos.close()
          }
        })
      }

      threads.foreach(_.start())
      threads.foreach(_.join())
    }
  }

  @Test
  def testByteArrayRead(): Unit = {
    val fptr = new RandomAccessFile(fileBody, "rw");
    fptr.setLength(1024 * 1024 * 10)
    for (len <- Array(1024, 10240, 102400, 1024000, 10240000)) {
      println(s"******reading $len bytes======")

      println(s"------RandomAccessFile.read------")
      var ix = 0
      Profiler.timing(true, 10) {
        ix += 1024

        fptr.seek(ix)
        val bytes = new Array[Byte](len);
        fptr.read(bytes)
        val buf = ByteBuffer.wrap(bytes)
        IOUtils.toByteArray(new ByteBufferInputStream(buf))
      }

      println(s"------mmap()------")
      val channel = fptr.getChannel
      ix = 0
      Profiler.timing(true, 10) {
        ix += 1024
        val buf = channel.map(FileChannel.MapMode.READ_ONLY, ix, len);
        IOUtils.toByteArray(new ByteBufferInputStream(buf))
      }
    }

    fptr.close()
  }

  @Test
  def testByteArrayWrite(): Unit = {
    for (len <- Array(1024, 10240, 102400, 1024000, 10240000)) {
      println(s"======writing $len bytes======")
      val bytes = new Array[Byte](len);

      println(s"------FileChannel.write------")
      //appender
      val appenderChannel = new FileOutputStream(fileBody, true).getChannel
      Profiler.timing(true, 10) {
        appenderChannel.write(ByteBuffer.wrap(bytes))
      }

      appenderChannel.close()

      println(s"------FileOutputStream.append------")

      //appender
      val fos = new FileOutputStream(fileBody, true)
      Profiler.timing(true, 10) {
        fos.write(bytes)
      }

      fos.close()

      println(s"------RandomAccessFile.write------")

      val fptr = new RandomAccessFile(fileBody, "rw");
      var ix = 0
      Profiler.timing(true, 10) {
        ix += 1024

        fptr.seek(ix)
        fptr.write(bytes)
      }

      fptr.close()
    }
  }

  @Test
  def testDirectByteBufferWrite(): Unit = {
    testByteBufferWrite(ByteBuffer.allocateDirect(_))
  }

  @Test
  def testHeapByteBufferWrite(): Unit = {
    testByteBufferWrite(ByteBuffer.allocate(_))
  }

  private def testByteBufferWrite(allocate: (Int) => ByteBuffer): Unit = {
    for (len <- Array(1024, 10240, 102400, 1024000, 10240000)) {
      println(s"======writing $len bytes======")
      Unpooled.buffer(1024)
      val buf = allocate(len)
      buf.put(new Array[Byte](len))
      buf.flip()

      println(s"------FileChannel.write------")
      //appender
      val appenderChannel = new FileOutputStream(fileBody, true).getChannel
      Profiler.timing(true, 10) {
        appenderChannel.write(buf.duplicate())
      }

      appenderChannel.close()

      println(s"------FileOutputStream.append------")

      //appender
      val fos = new FileOutputStream(fileBody, true)
      Profiler.timing(true, 10) {
        val bbuf = Unpooled.wrappedBuffer(buf.duplicate())
        val bs = new Array[Byte](bbuf.readableBytes())
        bbuf.readBytes(bs)
        fos.write(bs)
      }

      fos.close()

      println(s"------RandomAccessFile.write------")

      val fptr = new RandomAccessFile(fileBody, "rw");
      var ix = 0
      Profiler.timing(true, 10) {
        ix += 1024

        fptr.seek(ix)
        val bbuf = Unpooled.wrappedBuffer(buf.duplicate())
        val bs = new Array[Byte](bbuf.readableBytes())
        bbuf.readBytes(bs)
        fptr.write(bs)
      }

      fptr.close()
    }
  }
}
