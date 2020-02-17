package cn.graiph.regionfs.util

import java.nio.ByteBuffer

import io.netty.buffer.ByteBuf

/**
  * Created by bluejoe on 2020/2/15.
  */
trait ByteBufLike {
  def readInt(): Int;

  def writeInt(i: Int): Unit;

  def writeBytes(b: Array[Byte]): Unit;

  def readBytes(b: Array[Byte]): Unit;
}

class ByteBufLikeEx[T](src: T, bb: ByteBufLike) {
  def writeString(s: String) = {
    val arr = s.getBytes("utf-8")
    bb.writeInt(arr.length)
    bb.writeBytes(arr)

    src
  }

  def readString(): String = {
    val len = bb.readInt;
    val arr = new Array[Byte](len)
    bb.readBytes(arr)
    new String(arr, "utf-8")
  }
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

class ByteBufferEx(bb: ByteBuffer) extends
  ByteBufLikeEx(bb, new ByteBufferLike1(bb)) {

  def skipBytes(offset: Int): ByteBuffer = {
    bb.position(bb.position() + offset).asInstanceOf[ByteBuffer]
  }
}

class ByteBufEx(bb: ByteBuf) extends
  ByteBufLikeEx(bb, new ByteBufferLike2(bb)) {
}

object ByteBufferUtils {
  implicit def _toByteBufferEx(bb: ByteBuffer) = new ByteBufferEx(bb)

  implicit def _toByteBufEx(bb: ByteBuf) = new ByteBufEx(bb)
}
