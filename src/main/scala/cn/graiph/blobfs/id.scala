package cn.graiph.blobfs

import java.io.{ByteArrayOutputStream, DataOutputStream}

/**
  * Created by bluejoe on 2019/8/22.
  */

object FileId {
  def make(nodeId: Int, mimeCode: Int, localId: Int): FileId = {
    FileId(nodeId: Int, mimeCode: Int, localId: Int)
  }
}

//16 bytes
case class FileId(nodeId: Int, mimeCode: Int, localId: Int) extends Serializable {
  //[nn][nn][nn][nn][mm][mm][__][__][__][__][__][__][ll][ll][ll][ll]
  //n=node id, m=mime code, s=sequence id, i=offset id

  def asHexString(): String = {
    val baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)
    dos.writeInt(nodeId)
    dos.writeShort(mimeCode)
    dos.write(new Array[Byte](6))
    dos.writeInt(localId)

    baos.toByteArray.map((x: Byte) => {
      val s = (x & 0xff).toInt.toHexString.takeRight(2)
      if (s.length < 2)
        "0" + s
      else
        s
    }).mkString
  }
}