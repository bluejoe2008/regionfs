package regionfs

import org.grapheco.regionfs.util.{RetryStrategy, Rollbackable, TransactionFailedException, Transactional}
import org.junit.{Assert, Before, Test}

/**
  * Created by bluejoe on 2020/4/2.
  */
class TxTest {
  var (bluejoe, jason) = (100, 250)

  //jason pay money to bluejoe
  def step1(money: Int): Rollbackable[Int] = {
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

  def step2(money: Int): Rollbackable[Boolean] = {
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

  val tx = Transactional[Int, Int] {
    (money: Int) => {
      step1(money)
    }
  }.then[Boolean] {
    (money: Int) => {
      step2(money)
    }
  }

  @Before
  def reset(): Unit = {
    bluejoe = 100
    jason = 250
  }

  @Test
  def testNormal(): Unit = {
    val i = Transactional.run(tx, 50)
    Assert.assertEquals(true, i)
    Assert.assertEquals(150, bluejoe)
    Assert.assertEquals(200, jason)
  }

  @Test
  def testStep1Failed(): Unit = {
    //fails on 1-st step
    try {
      val i2 = Transactional.run(tx, 300)
      Assert.assertTrue(false)
    }
    catch {
      case e: TransactionFailedException =>
        e.printStackTrace()
        Assert.assertTrue(true)
      case _ =>
        Assert.assertTrue(false)
    }

    //value should be rolled back
    Assert.assertEquals((100, 250), (bluejoe, jason))
  }

  @Test
  def testStep2Failed(): Unit = {
    //fails on 2-st step
    try {
      val i2 = Transactional.run(tx, 220)
      Assert.assertTrue(false)
    }
    catch {
      case e: TransactionFailedException =>
        e.printStackTrace()
        Assert.assertTrue(true)
      case _ =>
        Assert.assertTrue(false)
    }

    //value should be rolled back
    Assert.assertEquals((100, 250), (bluejoe, jason))
  }

  @Test
  def testRetry(): Unit = {
    var counter = 0
    //fails on step3
    val tx2 = tx.then[String]((x: Boolean) => {
      counter += 1
      println(s"run step3...")
      if (counter < 5)
        Rollbackable.failure(new Exception("retry for success"))
      else
        Rollbackable.success("OK") {}
    })

    try {
      Transactional.run(tx2, 50, RetryStrategy.RUN_ONCE)
      Assert.assertTrue(false)
    }
    catch {
      case e: TransactionFailedException =>
        Assert.assertTrue(true)
      case e =>
        Assert.assertTrue(false)
    }

    val r2 = Transactional.run(tx2, 50, RetryStrategy.FOR_TIMES(5))
    Assert.assertEquals("OK", r2)
    Assert.assertEquals(counter, 5)
  }

  @Test
  def testRetry2(): Unit = {
    var counter = 0
    //fails on step0
    val tx2 = Transactional[Int, Int]((m: Int) => {
      counter += 1
      println(s"run step0...")
      if (counter < 5)
        Rollbackable.failure(new Exception("retry for success"))
      else
        Rollbackable.success(m) {}
    }).then(step1(_)).then(step2(_))

    try {
      Transactional.run(tx2, 50, RetryStrategy.RUN_ONCE)
      Assert.assertTrue(false)
    }
    catch {
      case e: TransactionFailedException =>
        Assert.assertTrue(true)
      case e =>
        Assert.assertTrue(false)
    }

    val r2 = Transactional.run(tx2, 50, RetryStrategy.FOR_TIMES(5))
    Assert.assertEquals(true, r2)
    Assert.assertEquals(counter, 5)
  }

  @Test
  def testRetry3(): Unit = {
    var counter = 0
    //fails on step1.5
    val tx2 = Transactional[Int, Int](step1(_)).then[Int]((m: Int) => {
      counter += 1
      println(s"run step1.5...")
      if (counter < 5)
        Rollbackable.failure(new Exception("retry for success"))
      else
        Rollbackable.success(m) {}
    }).then(step2(_))

    try {
      Transactional.run(tx2, 50, RetryStrategy.RUN_ONCE)
      Assert.assertTrue(false)
    }
    catch {
      case e: TransactionFailedException =>
        Assert.assertTrue(true)
      case e =>
        Assert.assertTrue(false)
    }

    val r2 = Transactional.run(tx2, 50, RetryStrategy.FOR_TIMES(5))
    Assert.assertEquals(true, r2)
    Assert.assertEquals(counter, 5)
  }
}
