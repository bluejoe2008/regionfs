package org.grapheco.regionfs.util

import java.io.{File, FileInputStream}
import java.nio.ByteBuffer

/**
  * Created by bluejoe on 2020/3/3.
  */
object ByteBufferConversions {
  implicit def wrapFileAsBuffer(file: File): ByteBuffer = {
    val channel = new FileInputStream(file).getChannel
    val buf = ByteBuffer.allocate(file.length().toInt)
    channel.read(buf)
    channel.close()
    buf.flip()

    buf
  }
}
