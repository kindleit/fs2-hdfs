package kindleit

import scala.util.{Try, Success, Failure}

package object fs2 {
  /** scala 2.11 does not have toEither method on Try class */
  implicit class TryToEither[T](val t: Try[T]) extends AnyVal {
    def toEither: Either[Throwable, T] = t match {
      case Success(t) => Right(t)
      case Failure(e) => Left(e)
    }
  }
}
