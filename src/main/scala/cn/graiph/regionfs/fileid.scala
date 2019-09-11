package cn.graiph.regionfs

import java.io.{ByteArrayOutputStream, DataOutputStream}

/**
  * Created by bluejoe on 2019/8/22.
  */

object FileId {
  def make(regionId: Long, localId: Long): FileId = {
    new FileId(regionId, localId)
  }
}

//16 bytes
case class FileId(regionId: Long, localId: Long) extends Serializable {
  //[rr][rr][rr][rr][rr][rr][rr][rr][ll][ll][ll][ll][ll][ll][ll][ll]

  def asHexString(): String = {
    val baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)
    dos.writeLong(regionId)
    dos.writeLong(localId)

    baos.toByteArray.map((x: Byte) => {
      val s = (x & 0xff).toInt.toHexString.takeRight(2)
      if (s.length < 2)
        "0" + s
      else
        s
    }).mkString
  }
}