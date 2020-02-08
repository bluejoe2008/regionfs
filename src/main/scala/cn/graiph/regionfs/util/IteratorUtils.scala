package cn.graiph.regionfs.util

/**
  * Created by bluejoe on 2020/2/7.
  */
object IteratorUtils {
  def concatIterators[T](makeSubIterator: => Option[Iterator[T]]): Iterator[T] = new Iterator[T] {
    var current = makeSubIterator;

    override def hasNext: Boolean = {
      if (current.isEmpty)
        false
      else {
        //eof
        if (!current.get.hasNext) {
          current = makeSubIterator;
        }

        current.get.hasNext
      }
    }

    override def next(): T = {
      if (!hasNext)
        throw new NoSuchElementException();

      current.get.next()
    }
  }
}
