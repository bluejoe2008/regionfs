package org.grapheco.regionfs.util

/**
  * Created by bluejoe on 2020/4/2.
  */
object Transactional {
  def apply(f: PartialFunction[Any, Rollbackable]) = new SingleTransactional(f)
}

trait Transactional {
  final def perform[T](x: Any, ctx: TransactionalContext): T = ctx.runWithRetry(this, x)

  def -->(f2: PartialFunction[Any, Rollbackable]): Transactional = andThen(f2)

  def andThen(f2: PartialFunction[Any, Rollbackable]): Transactional = new CompoundTransactional(this, f2)
}

case class SingleTransactional(f: (Any) => Rollbackable) extends Transactional {

}

case class CompoundTransactional(predecessor: Transactional, f2: (Any) => Rollbackable) extends Transactional {

}

trait RetryStrategy {
  def runWithRetry(f: (Any) => Rollbackable, x: Any): Rollbackable
}

object TransactionalContext {
  val DEFAULT: TransactionalContext = create(RetryStrategy.RUN_ONCE)

  def create(retryStrategy: RetryStrategy): TransactionalContext = new TransactionalContext(retryStrategy)
}

class TransactionalContext(retryStrategy: RetryStrategy) {
  def runWithRetry[T](tx: Transactional, x: Any): T = {
    _runWithRetry(tx, x) match {
      case Success(result, _) =>
        result.asInstanceOf[T]
      case Failure(e) =>
        throw new TransactionFailedException(e)
    }
  }

  private def _runWithRetry(f: (Any) => Rollbackable, x: Any): Rollbackable = retryStrategy.runWithRetry(f, x)

  private def _runWithRetry(tx: Transactional, x: Any): Rollbackable = tx match {
    case SingleTransactional(f) =>
      _runWithRetry(f, x)

    case CompoundTransactional(predecessor, f2) =>
      val r1 = _runWithRetry(predecessor, x)
      r1 match {
        case Success(y: Any, rollback) =>
          val r2 = _runWithRetry(f2, y)
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