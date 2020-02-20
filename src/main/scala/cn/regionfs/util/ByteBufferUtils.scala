package cn.regionfs.util

import java.nio.ByteBuffer

import io.netty.buffer.ByteBuf

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

  def readObject[X]()(implicit m: Manifest[X]): X = {
    val len = buf.readInt;
    val arr = new Array[Byte](len)
    buf.readBytes(arr)

    StreamUtils.deserializeObject(arr).asInstanceOf[X]
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
      buf.writeInt(bytes.length)
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
}

class ByteBufferLike2(bb: ByteBuf) extends ByteBufLike {
  def readInt(): Int = bb.readInt()

  def writeInt(i: Int): Unit = bb.writeInt(i)

  def writeBytes(b: Array[Byte]): Unit = bb.writeBytes(b)

  def readBytes(b: Array[Byte]): Unit = bb.readBytes(b)
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
