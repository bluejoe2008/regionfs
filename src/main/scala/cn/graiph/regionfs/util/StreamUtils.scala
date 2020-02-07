package cn.graiph.regionfs.util

import java.io.InputStream

/**
  * Created by bluejoe on 2020/2/7.
  */
object StreamUtils {
  def concatStreams(makeSubStream: => Option[InputStream]): InputStream = new InputStream {
    var current = makeSubStream;

    override def read(): Int = {
      if (current.isEmpty)
        -1
      else {
        val n = current.get.read();
        //eof
        if (n < 0) {
          current = makeSubStream;
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
