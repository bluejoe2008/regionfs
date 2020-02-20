package cn.regionfs.util

import java.io._

import org.apache.commons.codec.binary.Base64

/**
  * Created by bluejoe on 2020/2/7.
  */
object StreamUtils {
  def serializeObject(any: Any): Array[Byte] = {
    val baos = new ByteArrayOutputStream();
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(any)
    oos.close()
    baos.toByteArray
  }

  def deserializeObject(bytes: Array[Byte]): Any = {
    readObject(new ByteArrayInputStream(bytes))
  }

  def readObject(is: InputStream): Any = {
    val ois = new ObjectInputStream(is)
    ois.readObject()
  }

  val base64 = new Base64();

  def iterator2Stream(iter: Iterator[Byte]): InputStream = new InputStream() {
    override def read(): Int = {
      if (iter.hasNext) {
        iter.next()
      }
      else {
        -1
      }
    }
  }

  def concatChunks(produceChunk: => Option[InputStream]): InputStream = new InputStream {
    var current = produceChunk.getOrElse(null);

    override def read(): Int = {
      if (current == null)
        -1
      else {
        val n = current.read();
        //eof
        if (n < 0) {
          //read next chunk
          current = produceChunk.getOrElse(null);
          if (current == null)
            -1
          else
            current.read()
        }
        else {
          n
        }
      }
    }
  }
}
