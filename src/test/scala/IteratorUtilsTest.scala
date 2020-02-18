
import cn.regionfs.util.IteratorUtils
import org.junit.{Assert, Test}

/**
  * Created by bluejoe on 2020/2/9.
  */
class IteratorUtilsTest {
  @Test
  def test1(): Unit = {
    val it = IteratorUtils.concatIterators { (i) =>
      if (i == 6)
        None
      else
        Some((0 to i).iterator)
    }

    val list: Array[Int] = it.toArray
    println(list.toList)
    Assert.assertArrayEquals(Array[Int](0, 0, 1, 0, 1, 2, 0, 1, 2, 3, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 5), list);
  }
}
