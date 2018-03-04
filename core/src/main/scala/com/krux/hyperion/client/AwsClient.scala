package com.krux.hyperion.client

import com.amazonaws.auth.{DefaultAWSCredentialsProviderChain, STSAssumeRoleSessionCredentialsProvider}
import com.amazonaws.regions.Regions
import com.amazonaws.services.datapipeline.{DataPipelineClientBuilder, DataPipeline}
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder
import org.slf4j.LoggerFactory

import com.krux.hyperion.DataPipelineDefGroup
import com.krux.stubborn.policy.ExponentialBackoffAndJitter
import com.krux.stubborn.Retryable


trait AwsClient extends Retryable with ExponentialBackoffAndJitter {

  lazy val log = LoggerFactory.getLogger(getClass)

  def client: DataPipeline

  override def base: Int = 3000

  override def cap: Int = 24000 // theoretical max retry delay with 3 retries

}

object AwsClient {

  def getClient(regionId: Option[String] = None, roleArn: Option[String] = None)
    : DataPipeline = {

    val region: Regions =
      regionId.map(r => Regions.fromName(r)).getOrElse(Regions.US_EAST_1)

    lazy val defaultProvider = new DefaultAWSCredentialsProviderChain()

    lazy val stsProvider =
      roleArn.map { r =>
        new STSAssumeRoleSessionCredentialsProvider.Builder(r, "hyperion")
          .withStsClient(
            AWSSecurityTokenServiceClientBuilder
              .standard()
              .withCredentials(defaultProvider)
              .build()
          )
          .build()
      }

    DataPipelineClientBuilder
      .standard()
      .withCredentials(stsProvider.getOrElse(defaultProvider))
      .withRegion(region)
      .build()
  }

  def apply(
      pipelineDef: DataPipelineDefGroup,
      regionId: Option[String],
      roleArn: Option[String]
    ): AwsClientForDef =
    new AwsClientForDef(getClient(regionId, roleArn), pipelineDef)

}
