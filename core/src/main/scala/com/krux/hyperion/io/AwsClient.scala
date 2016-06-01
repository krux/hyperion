package com.krux.hyperion.io

import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.{ DefaultAWSCredentialsProviderChain, STSAssumeRoleSessionCredentialsProvider }
import com.amazonaws.regions.{ Region, Regions }
import com.amazonaws.services.datapipeline.DataPipelineClient
import org.slf4j.LoggerFactory

import com.krux.hyperion.AbstractDataPipelineDef


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

object AwsClient {

  def getClient(regionId: Option[String] = None, roleArn: Option[String] = None)
    : DataPipelineClient = {

    val region: Region =
      Region.getRegion(regionId.map(r => Regions.fromName(r)).getOrElse(Regions.US_EAST_1))
    val defaultProvider =
      new DefaultAWSCredentialsProviderChain()
    val stsProvider =
      roleArn.map(new STSAssumeRoleSessionCredentialsProvider(defaultProvider, _, "hyperion"))
    new DataPipelineClient(stsProvider.getOrElse(defaultProvider)).withRegion(region)
  }

  def apply(
      pipelineDef: AbstractDataPipelineDef,
      regionId: Option[String],
      roleArn: Option[String]
    ): AwsClientForDef = new AwsClientForDef(getClient(regionId, roleArn), pipelineDef)

}
