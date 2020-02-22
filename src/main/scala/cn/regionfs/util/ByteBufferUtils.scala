package cn.regionfs.util

import java.io.InputStream
import java.nio.ByteBuffer

import io.netty.buffer.{ByteBuf, ByteBufInputStream}
import net.neoremind.kraps.util.ByteBufferInputStream

/**
  * Created by bluejoe on 2020/2/15.
  */
class ByteBufLikeEx[T](src: T, buf: ByteBufLike) {
  def readString(): String = {
    val len = buf.readInt;
    val arr = new Array[Byte](len)
    buf.readBytes(arr)
    new String(arr, "utf-8")
  }

  def readObject(): Any = {
    val is = buf.createInputStream();
    val o = StreamUtils.readObject(is)
    is.close
    o;
  }

  //[length][...string...]
  def writeString(s: String): T = {
    val arr = s.getBytes("utf-8")
    buf.writeInt(arr.length)
    buf.writeBytes(arr)
    src
  }

  //[length][...object serlialization...]
  def writeObject(o: Any): T = {
    try {
      val bytes = StreamUtils.serializeObject(o)
      buf.writeBytes(bytes)
      src
    }
    catch {
      case e: Throwable =>
        throw new RuntimeException(s"failed to serialize object: ${o.getClass.getName}", e);
    }
  }
}

trait ByteBufLike {
  def createInputStream(): InputStream;

  def readInt(): Int;

  def writeInt(i: Int): Unit;

  def writeBytes(b: Array[Byte]): Unit;

  def readBytes(b: Array[Byte]): Unit;
}

class ByteBufferLike1(bb: ByteBuffer) extends ByteBufLike {
  def readInt(): Int = bb.getInt()

  def writeInt(i: Int): Unit = bb.putInt(i)

  def writeBytes(b: Array[Byte]): Unit = bb.put(b)

  def readBytes(b: Array[Byte]): Unit = bb.get(b)

  def createInputStream(): InputStream = new ByteBufferInputStream(bb)
}

class ByteBufferLike2(bb: ByteBuf) extends ByteBufLike {
  def readInt(): Int = bb.readInt()

  def writeInt(i: Int): Unit = bb.writeInt(i)

  def writeBytes(b: Array[Byte]): Unit = bb.writeBytes(b)

  def readBytes(b: Array[Byte]): Unit = bb.readBytes(b)

  def createInputStream(): InputStream = new ByteBufInputStream(bb)
}

class ByteBufferEx(buf: ByteBuffer) extends ByteBufLikeEx(buf, new ByteBufferLike1(buf)) {
  def skip(n: Int) = buf.position(buf.position() + n)
}

class ByteBufEx(buf: ByteBuf) extends ByteBufLikeEx(buf, new ByteBufferLike2(buf)) {

}

object ByteBufferUtils {
  implicit def _toByteBufEx(bb: ByteBuf) = new ByteBufEx(bb)

  implicit def _toByteBufferEx(bb: ByteBuffer) = new ByteBufferEx(bb)
}
