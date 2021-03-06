package regionfs

import org.grapheco.regionfs.util._
import org.junit.{Assert, Before, Test}

/**
  * Created by bluejoe on 2020/4/2.
  */
class TransactionTest {
  var (bluejoe, jason) = (100, 250)

  //jason pay money to bluejoe
  val step1: PartialFunction[Any, Rollbackable] = {
    case money: Int =>
      println(s"run step1...")

      val ov = jason
      if (jason < money) {
        Rollbackable.failure(new Exception("jason has no sufficient money!"))
      }
      else {
        jason -= money
        Rollbackable.success(money) {
          jason = ov
        }
      }
  }

  val step2: PartialFunction[Any, Rollbackable] = {
    case money: Int =>
      println(s"run step2...")

      val ov = bluejoe
      bluejoe += money
      if (bluejoe > 300) {
        bluejoe = ov
        Rollbackable.failure(new Exception("too much money for bluejoe!"))
      }
      else {
        Rollbackable.success(true) {
          bluejoe = ov
        }
      }
  }

  val tx = Atomic("-")(step1) --> Atomic("+")(step2)

  @Before
  def reset(): Unit = {
    bluejoe = 100
    jason = 250
  }

  @Test
  def testNormal(): Unit = {
    val i: Any = TransactionRunner.perform(tx, 50)
    Assert.assertEquals(true, i)
    Assert.assertEquals(150, bluejoe)
    Assert.assertEquals(200, jason)
  }

  @Test
  def testStep1Failed(): Unit = {
    //fails on 1-st step
    try {
      TransactionRunner.perform(tx, 300)
      Assert.assertTrue(false)
    }
    catch {
      case e: TransactionFailedException =>
        e.printStackTrace()
        Assert.assertTrue(true)
      case e: Throwable =>
        Assert.assertTrue(false)
    }

    //value should be rolled back
    Assert.assertEquals((100, 250), (bluejoe, jason))
  }

  @Test
  def testStep2Failed(): Unit = {
    //fails on 2-st step
    try {
      TransactionRunner.perform(tx, 220)
      Assert.assertTrue(false)
    }
    catch {
      case e: TransactionFailedException =>
        e.printStackTrace()
        Assert.assertTrue(true)
      case e: Throwable =>
        Assert.assertTrue(false)
    }

    //value should be rolled back
    Assert.assertEquals((100, 250), (bluejoe, jason))
  }

  @Test
  def testRetry(): Unit = {
    var counter = 0
    //fails on step3
    val tx2 = tx --> Atomic("step3") {
      case x: Boolean => {
        counter += 1
        println(s"run step3...")
        if (counter < 5)
          Rollbackable.failure(new Exception("retry for success"))
        else
          Rollbackable.success("OK") {
          }
      }
    }

    try {
      TransactionRunner.perform(tx2, 50, RetryStrategy.RUN_ONCE)
      Assert.assertTrue(false)
    }
    catch {
      case e: TransactionFailedException =>
        Assert.assertTrue(true)
      case e: Throwable =>
        e.printStackTrace()
        Assert.assertTrue(false)
    }

    val r2: Any = TransactionRunner.perform(tx2, 50, RetryStrategy.FOR_TIMES(5))
    Assert.assertEquals("OK", r2)
    Assert.assertEquals(counter, 5)
  }

  @Test
  def testRetry2(): Unit = {
    var counter = 0
    //fails on step0
    val tx2 = Atomic("step0") {
      case m: Int => {
        counter += 1
        println(s"run step0...")
        if (counter < 5)
          Rollbackable.failure(new Exception("retry for success"))
        else
          Rollbackable.success(m) {
          }
      }
    } --> Atomic("step1")(step1) --> Atomic("step2")(step2)

    try {
      TransactionRunner.perform(tx2, 50, RetryStrategy.RUN_ONCE)
      Assert.assertTrue(false)
    }
    catch {
      case e: TransactionFailedException =>
        Assert.assertTrue(true)
      case e: Throwable =>
        Assert.assertTrue(false)
    }

    val r2: Any = TransactionRunner.perform(tx2, 50, RetryStrategy.FOR_TIMES(5))
    Assert.assertEquals(true, r2)
    Assert.assertEquals(counter, 5)
  }

  @Test
  def testRetry3(): Unit = {
    var counter = 0
    //fails on step1.5
    val tx2 = Atomic("step1")(step1) --> Atomic("step1.5") {
      case m: Int => {
        counter += 1
        println(s"run step1.5...")
        if (counter < 5)
          Rollbackable.failure(new Exception("retry for success"))
        else
          Rollbackable.success(m) {
          }
      }
    } --> Atomic("step2")(step2)

    try {
      TransactionRunner.perform(tx2, 50)
      Assert.assertTrue(false)
    }
    catch {
      case e: TransactionFailedException =>
        Assert.assertTrue(true)
      case e: Throwable =>
        Assert.assertTrue(false)
    }

    val r2: Any = TransactionRunner.perform(tx2, 50, RetryStrategy.FOR_TIMES(5))
    Assert.assertEquals(true, r2)
    Assert.assertEquals(counter, 5)
  }
}
