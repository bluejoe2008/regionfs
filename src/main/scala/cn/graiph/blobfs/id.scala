package cn.graiph.blobfs

import java.io.{ByteArrayOutputStream, DataOutputStream}

/**
  * Created by bluejoe on 2019/8/22.
  */

object FileId {
  def make(nodeId: Int, mimeCode: Int, sequenceId: Int, innerId: Int): FileId = {
    FileId(nodeId: Int, mimeCode: Int, sequenceId: Int, innerId: Int);
  }
}

//16 bytes
case class FileId(nodeId: Int, mimeCode: Int, sequenceId: Int, innerId: Int) extends Serializable {
  //[nn][nn][mm][mm][__][__][ss][ss][ss][ss][ss][ss][ss][ss][oo][oo]
  //n=node id, m=mime code, s=sequence id, i=offset id

  def asHexString(): String = {
    val baos = new ByteArrayOutputStream();
    val dos = new DataOutputStream(baos);
    dos.writeShort(nodeId);
    dos.writeShort(mimeCode);
    dos.write(new Array[Byte](2));
    dos.writeLong(sequenceId);
    dos.writeShort(innerId);

    baos.toByteArray.map((x: Byte) => {
      val s = (x & 0xff).toInt.toHexString.takeRight(2)
      if (s.length < 2)
        "0" + s
      else
        s
    }).mkString
  }
}