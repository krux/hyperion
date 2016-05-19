package com.krux.hyperion.io

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.datapipeline.DataPipelineClient
import org.slf4j.LoggerFactory


trait AwsClient {

  lazy val log = LoggerFactory.getLogger(getClass)

  def client: DataPipelineClient

  protected def throttleRetry[A](func: => A, n: Int = 3): A = {
    if (n > 0)
      try {
        func
      } catch {
        // use startsWith becase the doc says the error code is called "Throttling" but sometimes
        // we see "ThrottlingException" instead
        case e: AmazonServiceException if e.getErrorCode().startsWith("Throttling") && e.getStatusCode == 400 =>
          log.warn(s"caught exception: ${e.getMessage}\n Retry after 5 seconds...")
          Thread.sleep(5000)
          throttleRetry(func, n - 1)
      }
    else
      func
  }


}
