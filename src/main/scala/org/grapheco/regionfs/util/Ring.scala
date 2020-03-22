package org.grapheco.regionfs.util

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bluejoe on 2020/3/9.
  */
class Ring[T]() {
  private val _buffer = ArrayBuffer[T]();
  private var pos = 0;

  def clear(): Unit = {
    _buffer.clear()
  }

  def -=(t: T) = {
    this.synchronized {
      val idx = _buffer.indexOf(t)

      if (idx != -1) {
        if (idx < pos) {
          pos -= 1
        }
      }
    }
  }

  def +=(t: T) = {
    this.synchronized {
      _buffer += t
    }
  }

  def ++=(t: Iterable[T]) = {
    this.synchronized {
      _buffer ++= t
    }
  }

  private def unsafeTakeOne(): T = {
    if (pos >= _buffer.size)
      pos = 0;

    val t = _buffer(pos)
    pos += 1
    t
  }

  def take(): T = {
    this.synchronized {
      unsafeTakeOne()
    }
  }

  def take(filter: (T) => Boolean): Option[T] = {
    this.synchronized {
      var n = _buffer.length;
      var t: T = null.asInstanceOf[T];
      do {
        t = unsafeTakeOne()
        n -= 1;
      } while (!filter(t) && n >= 0)

      if (n < 0)
        None
      else
        Some(t);
    }
  }
}
