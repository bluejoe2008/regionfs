package cn.bluejoe.regionfs.util

import java.lang.management.ManagementFactory

/**
  * Created by bluejoe on 2020/2/25.
  */
object ProcessUtils {
  def getCurrentPid(): Int = {
    val runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    return runtimeMXBean.getName().split("@")(0).toInt;
  }
}