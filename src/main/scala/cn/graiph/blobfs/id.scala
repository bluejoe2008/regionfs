package cn.graiph.blobfs

import java.io.{ByteArrayOutputStream, DataOutputStream}

/**
  * Created by bluejoe on 2019/8/22.
  */

object FileId {
  def make(regionId: Int, localId: Int): FileId = {
    new FileId(regionId: Int, localId: Int)
  }
}

//16 bytes
class FileId(regionId: Int, localId: Int) extends Serializable {
  //[nn][nn][nn][nn][mm][mm][__][__][__][__][__][__][ll][ll][ll][ll]
  //n=node id, m=mime code, s=sequence id, i=offset id

  def asHexString(): String = {
    val baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)
    dos.writeInt(regionId)
    dos.writeShort(0)
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