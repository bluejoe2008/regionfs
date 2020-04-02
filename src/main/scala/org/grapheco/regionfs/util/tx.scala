package org.grapheco.regionfs.util

/**
  * Created by bluejoe on 2020/4/2.
  */
object Transactional {
  def apply[X, Y](f: (X) => Rollbackable[Y]) = new Transactional[X, Y] {
    def perform(x: X, ctx: TransactionalContext): Rollbackable[Y] = f(x)
  }

  def run[X, Y](tx: Transactional[X, Y], x: X): Y = {
    val r = tx.perform(x, new TransactionalContext() {})
    r match {
      case Success(result: Y, _) =>
        result
      case Failure(e) =>
        throw new TransactionFailedException(e)
    }
  }
}

trait TransactionalContext {

}

trait Transactional[X, Y] {
  def perform(x: X, ctx: TransactionalContext): Rollbackable[Y]

  def then[Z](f: (Y) => Rollbackable[Z]): Transactional[X, Z] = {
    val predecessor = this
    new Transactional[X, Z] {
      def perform(x: X, ctx: TransactionalContext): Rollbackable[Z] = {
        val r1 = predecessor.perform(x, ctx)
        r1 match {
          case Success(y: Y, rollback) =>
            val r2 = f(y)
            r2 match {
              case Success(z: Z, rollback2) =>
                Rollbackable.success(z) {
                  rollback()
                  rollback2()
                }
              case Failure(e) =>
                rollback()
                Rollbackable.failure(e)
            }
          case Failure(e) =>
            Rollbackable.failure(e)
        }
      }
    }
  }
}

object Rollbackable {
  def success[Y](result: Y)(rollback: => Unit) = new Success(result, () => rollback)

  def failure[Y](e: Throwable) = new Failure[Y](e)
}

trait Rollbackable[Y] {

}

case class Failure[T](e: Throwable) extends Rollbackable[T] {

}

case class Success[Y](result: Y, rollback: () => Unit) extends Rollbackable[Y] {

}

class TransactionFailedException(cause: Throwable) extends
  RuntimeException(s"transaction failed", cause)