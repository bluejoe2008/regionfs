package regionfs

import java.io.{File, FileOutputStream, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import io.netty.buffer.Unpooled
import net.neoremind.kraps.util.ByteBufferInputStream
import org.apache.commons.io.{FileUtils, IOUtils}
import org.grapheco.commons.util.Profiler
import org.junit.Test

/**
  * Created by bluejoe on 2020/3/17.
  */
class LocalFileIOTest {
  Profiler.enableTiming = true
  val fileBody = new File("./testdata/output/abc")
  FileUtils.deleteDirectory(fileBody.getParentFile)
  fileBody.getParentFile.mkdirs()
  fileBody.createNewFile()

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
