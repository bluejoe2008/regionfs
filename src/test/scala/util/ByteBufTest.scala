package util

import java.nio.ByteBuffer

import cn.regionfs.util.ByteBufferUtils._
import cn.regionfs.util.Profiler
import cn.regionfs.util.Profiler._
import io.netty.buffer.Unpooled
import org.apache.spark.network.buffer.NioManagedBuffer
import org.junit.{Assert, Test}

/**
  * Created by bluejoe on 2020/2/14.
  */
class ByteBufTest {
  Profiler.enableTiming = true

  @Test
  def testByteBuffer(): Unit = {
    val bb = timing(true) {
      ByteBuffer.allocate(20)
    }
    println(bb.array(), bb.arrayOffset(), bb.position(), bb.remaining())
    bb.put(9.toByte)
    bb.writeString("hello")
    bb.putInt(100)
    println(bb.array(), bb.arrayOffset(), bb.position(), bb.remaining())
    bb.skip(2)
    bb.putInt(200)
    println(bb.array(), bb.arrayOffset(), bb.position(), bb.remaining())
    bb.flip();
    Assert.assertEquals(9, bb.get);
    Assert.assertEquals("hello", bb.readString());
    Assert.assertEquals(100, bb.getInt());
    bb.skip(2)
    Assert.assertEquals(200, bb.getInt());
  }

  @Test
  def testByteBuf(): Unit = {
    //63us
    timing(true) {
      ByteBuffer.allocate(10000)
    }

    //200ms
    timing(true) {
      //expansive time cost!!!
      Unpooled.wrappedBuffer(ByteBuffer.allocate(10000))
    }

    //2ms
    timing(true) {
      Unpooled.compositeBuffer()
    }

    //1ms
    timing(true) {
      Unpooled.buffer()
    }

    //39us
    val buf = timing(true) {
      Unpooled.buffer(10000)
    }

    //74us
    timing(true) {
      Unpooled.buffer(10000).nioBuffer()
    }

    val bb = timing(true) {
      Unpooled.buffer(10)
    }

    println(bb.array(), bb.arrayOffset(), bb.readerIndex(), bb.writerIndex(), bb.readableBytes())

    bb.writeInt(100)
    println(bb.array(), bb.arrayOffset(), bb.readerIndex(), bb.writerIndex(), bb.readableBytes())

    bb.writeZero(20)
    println(bb.array(), bb.arrayOffset(), bb.readerIndex(), bb.writerIndex(), bb.readableBytes())

    bb.readInt()
    println(bb.array(), bb.arrayOffset(), bb.readerIndex(), bb.writerIndex(), bb.readableBytes())
  }

  @Test
  def testManagedBuffer(): Unit = {
    Unpooled.buffer(1)

    val buf = timing(true) {
      Unpooled.buffer(10000)
    }

    timing(true) {
      buf.writeBytes(new Array[Byte](1024 * 1024))
    }

    val mb = timing(true) {
      new NioManagedBuffer(buf.nioBuffer())
    }

    timing(true) {
      mb.createInputStream()
    }

    timing(true) {
      mb.convertToNetty()
    }

    timing(true) {
      mb.nioByteBuffer()
    }
  }
}
