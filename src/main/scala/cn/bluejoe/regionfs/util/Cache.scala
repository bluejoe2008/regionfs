package cn.bluejoe.regionfs.util

import scala.collection.mutable

/**
  * Created by bluejoe on 2020/2/23.
  */
trait Cache[K, V] {
  def get(key: K): Option[V]

  def put(key: K, value: V)
}

class FixSizedCache[K, V](capacity: Int) extends Cache[K, V] {
  val queue = mutable.ListMap[K, V]();

  def get(key: K): Option[V] = queue.get(key)

  def put(key: K, value: V) = {
    while (queue.size >= capacity) {
      queue.drop(capacity - queue.size + 10)
    }

    queue(key) = value
  }
}
