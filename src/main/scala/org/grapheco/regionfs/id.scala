package org.grapheco.regionfs

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.apache.commons.codec.binary.Base64

/**
  * Created by bluejoe on 2019/8/22.
  */

object FileId {
  val base64 = new Base64()

  def make(regionId: Long, localId: Long): FileId = {
    new FileId(regionId, localId)
  }

  def toBase64String(id: FileId): String = {
    val baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)
    dos.writeLong(id.regionId)
    dos.writeLong(id.localId)

    base64.encodeAsString(baos.toByteArray)
  }

  def fromBase64String(text: String): FileId = {
    val bytes = base64.decode(text)
    val dis = new DataInputStream(new ByteArrayInputStream(bytes))
    make(dis.readLong(), dis.readLong())
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
  def getBase64String() = FileId.toBase64String(this)
}