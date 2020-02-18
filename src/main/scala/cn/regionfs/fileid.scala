package cn.regionfs

import java.io.{ByteArrayOutputStream, DataOutputStream}

/**
  * Created by bluejoe on 2019/8/22.
  */

object FileId {
  def make(regionId: Long, localId: Long): FileId = {
    new FileId(regionId, localId)
  }
}

/**
  * a FileId represents a 16 bytes id assigned to a blob
  * layout:
  * [rr][rr][rr][rr][rr][rr][rr][rr][ll][ll][ll][ll][ll][ll][ll][ll]
  * rr=regionId, ll=localId
  * localId is an unique id in a region, often be an integer index
  */
case class FileId(regionId: Long, localId: Long) extends Serializable {

  /**
    * print this FileId as a string
    */
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