package org.grapheco.regionfs.util

/**
  * Created by bluejoe on 2020/4/8.
  */
class Version(major: Int, minor: Int, patch: Int) extends Comparable[Version] {

  override def toString = s"${major}.${minor}.${patch}"

  def toLong = major * 1000000L + minor * 10000 + patch

  override def compareTo(o: Version): Int = this.toLong.compareTo(o.toLong)
}

object Version {
  def apply(num: Long) = new Version((num / 1000000).toInt, ((num / 10000) % 100).toInt, (num % 10000).toInt)

  def apply(num: String) = {
    val list = num.split("\\.")
    new Version(list(0).toInt, list(1).toInt, list(2).toInt)
  }

  def apply(major: Int, minor: Int, patch: Int) = new Version(major: Int, minor: Int, patch: Int)
}
