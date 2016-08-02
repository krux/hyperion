package com.krux.hyperion.client

import scala.util.Random

import com.amazonaws.AmazonServiceException
import org.slf4j.Logger


/**
 * Performs retry on throttle
 */
trait Retry {

  def log: Logger

  def maxRetry: Int

  implicit class Retryable[A](action: => A) {

    def retry(n: Int = 0): A = {
      if (n < maxRetry)
        try {
          action
        } catch {
          // use startsWith becase the doc says the error code is called "Throttling" but sometimes
          // we see "ThrottlingException" instead
          case e: AmazonServiceException if e.getErrorCode.startsWith("Throttling") && e.getStatusCode == 400 =>
            val retryDelay = Random.nextInt(Math.pow(2, (n + 1)).toInt) + 5
            log.warn(s"caught exception: ${e.getMessage}\n Retry (No. $n) after $retryDelay seconds...")
            Thread.sleep(retryDelay * 1000)
            retry(n + 1)
        }
      else
        action
    }

  }

}
