package cn.regionfs.util

/**
  * Created by bluejoe on 2020/2/11.
  */
object Profiler extends Logging {
  var enableTiming = false;

  def timing[T](enabled: Boolean = true, repeat: Int = 1)(runnable: => T): T = if (enableTiming & enabled) {
    val t1 = System.nanoTime()
    var result: T = null.asInstanceOf[T];
    for (i <- 1 to repeat) {
      result = runnable
    }

    val t2 = System.nanoTime()

    println(new Exception().getStackTrace()(1).toString)

    val elapsed = (t2 - t1) / repeat;
    if (elapsed > 1000000) {
      println(s"time: ${elapsed / 1000000}ms")
    }
    else {
      println(s"time: ${elapsed / 1000}us")
    }

    result
  }
  else {
    runnable
  }
}
