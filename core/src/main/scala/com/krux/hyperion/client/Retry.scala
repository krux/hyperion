package com.krux.hyperion.client

import com.amazonaws.AmazonServiceException
import org.slf4j.Logger

/**
 * Performs retry on throttle
 */
trait Retry {

  def log: Logger

  def maxRetry = 3

  def retryDelay = 5000  // in miliseconds

  implicit class Retryable[A](action: => A) {

    def retry(n: Int = maxRetry): A = {
      if (n > 0)
        try {
          action
        } catch {
          // use startsWith becase the doc says the error code is called "Throttling" but sometimes
          // we see "ThrottlingException" instead
          case e: AmazonServiceException if e.getErrorCode().startsWith("Throttling") && e.getStatusCode == 400 =>
            log.warn(s"caught exception: ${e.getMessage}\n Retry after 5 seconds...")
            Thread.sleep(retryDelay)
            retry(n - 1)
        }
      else
        action
    }

  }

}
