package org.grapheco.regionfs.util

import java.util.concurrent.atomic.AtomicLong

/**
  * Created by bluejoe on 2020/4/2.
  */
object Transactional {
  val idgen = new AtomicLong(0)

  def apply[X, Y](f: (X) => Rollbackable[Y]) = new Transactional[X, Y] {
    def perform(x: X, ctx: TransactionalContext): Rollbackable[Y] = f(x)
  }

  def run[X, Y](tx: Transactional[X, Y], x: X, retryStrategy: RetryStrategy = RetryStrategy.RUN_ONCE): Y = {
    val ctx = new TransactionalContext(idgen.incrementAndGet(), retryStrategy)
    val r = ctx.runWithRetry(tx.perform(_: X, ctx), x)
    r match {
      case Success(result: Y, _) =>
        result
      case Failure(e) =>
        throw new TransactionFailedException(e)
    }
  }
}

private class TransactionalContext(transactionId: Long, retryStrategy: RetryStrategy) {
  def runWithRetry[X, Y](f: (X) => Rollbackable[Y], x: X): Rollbackable[Y] = retryStrategy.runWithRetry(f, x)
}

object RetryStrategy {
  val RUN_ONCE = new RetryStrategy() {
    def runWithRetry[X, Y](f: (X) => Rollbackable[Y], x: X): Rollbackable[Y] = {
      f(x)
    }
  }

  def FOR_TIMES(n: Int) = new RetryStrategy() {
    def runWithRetry[X, Y](f: (X) => Rollbackable[Y], x: X): Rollbackable[Y] = {
      var r = null.asInstanceOf[Rollbackable[Y]]
      var i = 0
      do {
        r = f(x)
        i += 1
      } while (r.isInstanceOf[Failure[_]] && i < n)
      r
    }
  }

  def WITHIN_TIME(ms: Int) = new RetryStrategy() {
    def runWithRetry[X, Y](f: (X) => Rollbackable[Y], x: X): Rollbackable[Y] = {
      var r = null.asInstanceOf[Rollbackable[Y]]
      val timeout = System.currentTimeMillis() + ms
      do {
        r = f(x)
      } while (r.isInstanceOf[Failure[_]] && System.currentTimeMillis() < timeout)
      r
    }
  }
}

trait RetryStrategy {
  def runWithRetry[X, Y](f: (X) => Rollbackable[Y], x: X): Rollbackable[Y]
}

trait Transactional[X, Y] {
  def perform(x: X, ctx: TransactionalContext): Rollbackable[Y]

  def then[Z](f: (Y) => Rollbackable[Z]): Transactional[X, Z] = {
    val predecessor = this
    new Transactional[X, Z] {
      def perform(x: X, ctx: TransactionalContext): Rollbackable[Z] = {
        val r1 = ctx.runWithRetry(predecessor.perform(_: X, ctx), x)
        r1 match {
          case Success(y: Y, rollback) =>
            val r2 = ctx.runWithRetry(f, y)
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