package cn.graiph.regionfs.util

import java.io.InputStream

/**
  * Created by bluejoe on 2020/2/7.
  */
object StreamUtils {
  def of(iter: Iterator[Byte]): InputStream = new InputStream {
    override def read(): Int = {
      if (iter.hasNext) {
        iter.next()
      }
      else {
        -1
      }
    }
  }

  def concatStreams(produceSubStream: => Option[InputStream]): InputStream = new InputStream {
    var current = produceSubStream;

    override def read(): Int = {
      if (current.isEmpty)
        -1
      else {
        val n = current.get.read();
        //eof
        if (n < 0) {
          current = produceSubStream;
          if (current.isEmpty)
            -1
          else
            current.get.read()
        }
        else {
          n
        }
      }
    }
  }
}
