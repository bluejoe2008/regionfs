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
    val idx = _buffer.indexOf(t)
    if (idx != -1) {
      if (idx < pos) {
        pos -= 1
      }
    }
  }

  def +=(t: T) = {
    _buffer += t
  }

  def ++=(t: Iterable[T]) = {
    _buffer ++= t
  }

  def !(): T = {
    if (pos == _buffer.size)
      pos = 0;

    val t = _buffer(pos)
    pos += 1

    t
  }

  def !(filter: (T) => Boolean): Option[T] = {
    var n = _buffer.length;
    var t: T = null.asInstanceOf[T];
    do {
      t = this.!()
      n -= 1;
    } while (!filter(t) && n > 0)

    if (n == 0)
      None
    else
      Some(t);
  }
}
