package org.grapheco.regionfs.server

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.channels.FileChannel

import org.apache.commons.io.IOUtils

/**
  * Created by bluejoe on 2020/6/2.
  */
trait Fork {
  def success(): Unit
}

object Fork {
  val empty = new Fork {
    override def success(): Unit = {}
  }
}

trait Forkable {
  def fork(): Fork
}

trait FileBasedWAL extends Forkable {
  protected def makeFile(postfix: String): File

  var _appender = new FileOutputStream(makeFile(""), true).getChannel

  def append[T](f: (FileChannel) => T): T = {
    _appender.synchronized {
      f(_appender)
    }
  }

  def fork(): Fork = {
    //no new contents
    if (_appender.position() == 0) {
      Fork.empty
    }
    else {
      val file2 = makeFile(".2")

      _appender.synchronized {
        _appender.close()

        //log-->log.2
        makeFile("").renameTo(file2)
        //create log
        makeFile("").createNewFile()
        _appender = new FileOutputStream(makeFile(""), true).getChannel
      }

      //if file.1 exists
      val file1 = makeFile(".1")
      if (!file1.exists()) {
        //log.2-->log.1
        file2.renameTo(file1)
      }
      else {
        //combine
        val fos = new FileOutputStream(file1, true)
        val fis = new FileInputStream(file2)
        //log.2+->log.1
        IOUtils.copy(fis, fos)
        fis.close()
        fos.close()
        file2.delete()
      }

      new Fork {
        def success() = {
          file1.delete()
        }
      }
    }
  }

  def close(): Unit = {
    _appender.synchronized {
      _appender.close()
    }
  }
}
