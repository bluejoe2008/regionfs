package org.grapheco.regionfs.util

import java.io.InputStream
import java.nio.ByteBuffer
import java.util.zip.{CRC32, CheckedInputStream}

/**
  * Created by bluejoe on 2020/3/3.
  */
object CrcUtils {
  def computeCrc32(buf: ByteBuffer): Long = {
    //get crc32
    assert(buf.remaining() > 0)
    val crc32 = new CRC32()
    crc32.update(buf)
    crc32.getValue
  }

  //NOTE: this method is very slow
  def computeCrc32(stream: => InputStream): Long = {
    //get crc32
    val crc32 = new CRC32()
    val cis = new CheckedInputStream(stream, crc32)

    while (cis.read() != -1) {
    }

    val crc32Value = crc32.getValue
    cis.close()
    crc32Value
  }
}
