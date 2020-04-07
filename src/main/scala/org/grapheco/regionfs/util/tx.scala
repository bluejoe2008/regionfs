package org.grapheco.regionfs.util

import org.grapheco.commons.util.Logging
import org.grapheco.regionfs.util.Rollbackable.{Failure, Success}

/**
  * Created by bluejoe on 2020/4/2.
  */
object Transactional {
  def apply(f: PartialFunction[Any, Rollbackable]) = new SimpleTransactional(f)
}

trait Transactional {
  final def perform(x: Any, ctx: TransactionalContext): Any = ctx.runWithRetry(this, Some(x))

  def &(f2: PartialFunction[Any, Rollbackable]): Transactional = andThen(f2)

  def andThen(f2: PartialFunction[Any, Rollbackable]): Transactional = new CompoundTransactional(this, f2)
}

case class SimpleTransactional(f: (Any) => Rollbackable) extends Transactional {

}

case class CompoundTransactional(predecessor: Transactional, f2: (Any) => Rollbackable) extends Transactional {

}

trait RetryStrategy {
  def runWithRetry(f: => Rollbackable): Rollbackable
}

object TransactionalContext {
  val DEFAULT: TransactionalContext = create(RetryStrategy.RUN_ONCE)

  def create(retryStrategy: RetryStrategy): TransactionalContext = new TransactionalContext(retryStrategy)
}

class TransactionalContext(retryStrategy: RetryStrategy) extends Logging {
  def runWithRetry(tx: Transactional, x: Option[Any]): Any = {
    _runWithRetry(tx, x) match {
      case Success(result, _) =>
        result
      case Failure(e) =>
        if (logger.isTraceEnabled()) {
          logger.trace(s"failed to execute transaction", e)
        }

        throw new TransactionFailedException(e)
    }
  }

  private def _runWithRetry(f: => Rollbackable): Rollbackable = retryStrategy.runWithRetry(f)

  private def _runWithRetry(tx: Transactional, input: Option[Any]): Rollbackable = (tx, input) match {
    case (SimpleTransactional(f), Some(x)) =>
      _runWithRetry(f(x))

    case (CompoundTransactional(predecessor, f2), Some(x)) =>
      val r1 = _runWithRetry(predecessor, input)
      r1 match {
        case Success(y: Any, rollback) =>
          val r2 = _runWithRetry(f2(y))
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