package cn.graiph.regionfs.util

/**
  * Created by bluejoe on 2020/2/11.
  */
object Profiler extends Logging {
  var enableTiming = true;

  def timing[T](enabled: Boolean = true)(runnable: => T): T = if (enableTiming & enabled) {
    val t1 = System.nanoTime()
    val result = runnable;
    val t2 = System.nanoTime()

    println(new Exception().getStackTrace()(1).toString)
    val elapsed = t2 - t1;
    if (elapsed > 1000000) {
      println(s"time: ${(t2 - t1) / 1000000}ms")
    }
    else {
      println(s"time: ${(t2 - t1) / 1000}us")
    }

    result
  }
  else {
    runnable
  }
}
