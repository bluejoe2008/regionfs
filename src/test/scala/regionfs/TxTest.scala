package regionfs

import org.grapheco.regionfs.util.{Rollbackable, TransactionFailedException, Transactional}
import org.junit.{Assert, Test}

/**
  * Created by bluejoe on 2020/4/2.
  */
class TxTest {
  @Test
  def test1(): Unit = {

    var (bluejoe, jason) = (100, 250)

    //jason pay money to bluejoe
    val tx = Transactional[Int, Int] {
      //step1
      (i: Int) => {
        val ov = jason
        if (jason < i) {
          Rollbackable.failure(new Exception("jason has no sufficient money!"))
        }
        else {
          jason -= i
          Rollbackable.success(i) {
            jason = ov
          }
        }
      }
    }.then[Boolean] {
      //step2
      (i: Int) => {
        val ov = bluejoe
        bluejoe += i
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
    }

    val i = Transactional.run(tx, 50)
    Assert.assertEquals(true, i)
    Assert.assertEquals(150, bluejoe)
    Assert.assertEquals(200, jason)

    //fails on 1-st step
    bluejoe = 100
    jason = 250

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
}
