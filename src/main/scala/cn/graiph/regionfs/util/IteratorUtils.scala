package cn.graiph.regionfs.util

/**
  * Created by bluejoe on 2020/2/7.
  */
object IteratorUtils {
  def concatIterators[T](makeSubIterator: (Int) => Option[Iterator[T]]): Iterator[T] = new Iterator[T] {
    var index = 0;
    var current = makeSubIterator(index);

    override def hasNext: Boolean = {
      if (current.isEmpty)
        false
      else {
        if (current.get.hasNext) {
          true
        }
        else {
          index += 1
          current = makeSubIterator(index)
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
