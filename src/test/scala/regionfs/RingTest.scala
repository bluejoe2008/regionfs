package regionfs

import org.grapheco.regionfs.util.Ring
import org.junit.{Assert, Test}

/**
  * Created by bluejoe on 2020/3/12.
  */
class RingTest {
  @Test
  def test1() {
    val ring = new Ring[Int]();
    ring ++= (1 to 3);
    Assert.assertEquals(1, ring.!)
    Assert.assertEquals(2, ring.!)
    Assert.assertEquals(3, ring.!)
    Assert.assertEquals(1, ring.!)
  }

  @Test
  def test2() {
    val ring = new Ring[Int]();
    ring ++= (1 to 3);
    Assert.assertEquals(Some(2), ring.!(_ % 2 == 0))
    Assert.assertEquals(Some(2), ring.!(_ % 2 == 0))
    Assert.assertEquals(Some(3), ring.!(_ % 2 == 1))
    Assert.assertEquals(Some(1), ring.!(_ % 2 == 1))
    Assert.assertEquals(Some(3), ring.!(_ % 3 == 0))
    Assert.assertEquals(None, ring.!(_ % 4 == 0))
  }
}
