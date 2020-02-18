package cn.regionfs.util

/**
  * Created by bluejoe on 2020/2/7.
  */
object IteratorUtils {
  def concatIterators[T](produceSubIterator: (Int) => Option[Iterator[T]]): Iterator[T] = new Iterator[T] {
    var index = 0;
    var current = produceSubIterator(index);

    override def hasNext: Boolean = {
      //None
      if (current.isEmpty)
        false
      else {
        //Non-None
        if (current.get.hasNext) {
          true
        }
        else {
          index += 1
          current = produceSubIterator(index)
          hasNext
        }
      }
    }

    override def next(): T = {
      if (!hasNext)
        throw new NoSuchElementException();

      current.get.next()
    }
  }
}
