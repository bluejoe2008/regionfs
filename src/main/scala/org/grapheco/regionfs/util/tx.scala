package org.grapheco.regionfs.util

import org.grapheco.commons.util.Logging
import org.grapheco.regionfs.util.Rollbackable.{Failure, Success}

/**
  * Created by bluejoe on 2020/4/2.
  */
object Atomic {
  def apply(description: String)(f: PartialFunction[Any, Rollbackable]) = new SingleAtomic(description, f)
}

trait Atomic {
  def -->(a2: SingleAtomic): Atomic = andThen(a2)

  def andThen(a2: SingleAtomic): Atomic = new CompositeAtomic(this, a2)
}

case class SingleAtomic(description: String, logic: (Any) => Rollbackable) extends Atomic {

}

case class CompositeAtomic(a1: Atomic, a2: SingleAtomic) extends Atomic {

}

trait RetryStrategy {
  def runWithRetry(f: => Rollbackable): Rollbackable
}

object TransactionRunner extends Logging {

  def perform(tx: Atomic, x: Any, retryStrategy: RetryStrategy = RetryStrategy.RUN_ONCE): Any = runWithRetry(tx, Some(x), retryStrategy)

  private def runWithRetry(tx: Atomic, x: Option[Any], retryStrategy: RetryStrategy): Any = {
    _runWithRetry(tx, x, retryStrategy) match {
      case Success(result, _) =>
        result
      case Failure(e) =>
        if (logger.isTraceEnabled()) {
          logger.trace(s"failed to execute transaction", e)
        }

        throw new TransactionFailedException(e)
    }
  }

  private def _runWithRetry(tx: Atomic, input: Option[Any], retryStrategy: RetryStrategy): Rollbackable = (tx, input) match {
    case (SingleAtomic(description, logic), Some(x)) =>
      retryStrategy.runWithRetry(logic(x))

    case (CompositeAtomic(a1, SingleAtomic(description, logic)), Some(x)) =>
      val r1 = _runWithRetry(a1, input, retryStrategy)
      r1 match {
        case Success(y: Any, rollback) =>
          val r2 = retryStrategy.runWithRetry(logic(y))
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

object RetryStrategy {
  val RUN_ONCE = new RetryStrategy() {
    def runWithRetry(f: => Rollbackable): Rollbackable = {
      f
    }
  }

  def FOR_TIMES(n: Int) = new RetryStrategy() {
    def runWithRetry(f: => Rollbackable): Rollbackable = {
      var r = null.asInstanceOf[Rollbackable]
      var i = 0
      do {
        r = f
        i += 1
      } while (r.isInstanceOf[Failure] && i < n)
      r
    }
  }

  def WITHIN_TIME(ms: Int) = new RetryStrategy() {
    def runWithRetry(f: => Rollbackable): Rollbackable = {
      var r = null.asInstanceOf[Rollbackable]
      val timeout = System.currentTimeMillis() + ms
      do {
        r = f
      } while (r.isInstanceOf[Failure] && System.currentTimeMillis() < timeout)
      r
    }
  }
}

object Rollbackable {
  def success(result: Any)(rollback: => Unit) = new Success(result, () => rollback)

  def failure(e: Throwable) = new Failure(e)

  case class Failure(e: Throwable) extends Rollbackable {

  }

  case class Success(result: Any, rollback: () => Unit) extends Rollbackable {

  }

}

trait Rollbackable {
  def map(f: PartialFunction[Any, Rollbackable]): Rollbackable = {
    this match {
      case Success(result: Any, rollback) =>
        f(result) match {
          case Success(result2: Any, rollback2) =>
            Success(result2: Any, {
              rollback
              rollback2
            })

          case Failure(e: Throwable) =>
            rollback
            Failure(e)
        }

      case Failure(e: Throwable) => this
    }
  }
}

class TransactionFailedException(cause: Throwable) extends
  RuntimeException(s"transaction failed", cause)