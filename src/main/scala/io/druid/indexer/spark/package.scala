package io.druid.indexer

import scala.util.Failure
import scala.util.Random
import scala.util.Success
import scala.util.Try

package object spark {
  @annotation.tailrec
  def retryWithExponentialBackoff[T](n: Int, sleepMillis: Long)(fn: => T): T = {
    Try { fn } match {
      case Success(x) => x
      case _ if n > 1 => Thread.sleep(sleepMillis); retryWithExponentialBackoff(n - 1, 2 * sleepMillis)(fn)
      case Failure(e) => throw e
    }
  }

  /**
    * Retry on any non-fatal exception with exponential backoff starting from 1000-2000 millis of sleep
    */
  def retryWithExponentialBackoff[T](n: Int)(fn: => T): T = {
    retryWithExponentialBackoff(n, ((1 + Random.nextDouble()) * 1000).toLong)(fn)
  }
}
