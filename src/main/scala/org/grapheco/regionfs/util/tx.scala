package org.grapheco.regionfs.util

import java.util.concurrent.atomic.AtomicLong

/**
  * Created by bluejoe on 2020/4/2.
  */
object Transactional {
  val idgen = new AtomicLong(0)

  def apply(f: PartialFunction[Any, Rollbackable]) = new Transactional {
    def perform(x: Any, ctx: TransactionalContext): Rollbackable = f(x)
  }

  def run[T](tx: Transactional, x: Any, retryStrategy: RetryStrategy = RetryStrategy.RUN_ONCE): T = {
    val ctx = new TransactionalContext(idgen.incrementAndGet(), retryStrategy)
    val r = ctx.runWithRetry(tx.perform(_, ctx), x)
    r match {
      case Success(result, _) =>
        result.asInstanceOf[T]
      case Failure(e) =>
        throw new TransactionFailedException(e)
    }
  }
}

private class TransactionalContext(transactionId: Long, retryStrategy: RetryStrategy) {
  def runWithRetry(f: (Any) => Rollbackable, x: Any): Rollbackable = retryStrategy.runWithRetry(f, x)
}

object RetryStrategy {
  val RUN_ONCE = new RetryStrategy() {
    def runWithRetry(f: (Any) => Rollbackable, x: Any): Rollbackable = {
      f(x)
    }
  }

  def FOR_TIMES(n: Int) = new RetryStrategy() {
    def runWithRetry(f: (Any) => Rollbackable, x: Any): Rollbackable = {
      var r = null.asInstanceOf[Rollbackable]
      var i = 0
      do {
        r = f(x)
        i += 1
      } while (r.isInstanceOf[Failure] && i < n)
      r
    }
  }

  def WITHIN_TIME(ms: Int) = new RetryStrategy() {
    def runWithRetry(f: (Any) => Rollbackable, x: Any): Rollbackable = {
      var r = null.asInstanceOf[Rollbackable]
      val timeout = System.currentTimeMillis() + ms
      do {
        r = f(x)
      } while (r.isInstanceOf[Failure] && System.currentTimeMillis() < timeout)
      r
    }
  }
}

trait RetryStrategy {
  def runWithRetry(f: (Any) => Rollbackable, x: Any): Rollbackable
}

trait Transactional {
  def perform(x: Any, ctx: TransactionalContext): Rollbackable

  def then(f: PartialFunction[Any, Rollbackable]): Transactional = {
    val predecessor = this
    new Transactional {
      def perform(x: Any, ctx: TransactionalContext): Rollbackable = {
        val r1 = ctx.runWithRetry(predecessor.perform(_, ctx), x)
        r1 match {
          case Success(y: Any, rollback) =>
            val r2 = ctx.runWithRetry(f, y)
            r2 match {
              case Success(z: Any, rollback2) =>
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
  def success(result: Any)(rollback: => Unit) = new Success(result, () => rollback)

  def failure(e: Throwable) = new Failure(e)
}

trait Rollbackable {

}

case class Failure(e: Throwable) extends Rollbackable {

}

case class Success(result: Any, rollback: () => Unit) extends Rollbackable {

}

class TransactionFailedException(cause: Throwable) extends
  RuntimeException(s"transaction failed", cause)