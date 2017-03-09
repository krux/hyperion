package com.krux.hyperion.client

import com.amazonaws.auth.{DefaultAWSCredentialsProviderChain, STSAssumeRoleSessionCredentialsProvider}
import com.amazonaws.regions.Regions
import com.amazonaws.services.datapipeline.{DataPipelineClientBuilder, DataPipeline}
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder
import org.slf4j.LoggerFactory

import com.krux.hyperion.DataPipelineDefGroup


trait AwsClient extends Retry {

  lazy val log = LoggerFactory.getLogger(getClass)

  def client: DataPipeline

}

object AwsClient {

  def getClient(regionId: Option[String] = None, roleArn: Option[String] = None)
    : DataPipeline = {

    val region: Regions =
      regionId.map(r => Regions.fromName(r)).getOrElse(Regions.US_EAST_1)
    val defaultProvider =
      new DefaultAWSCredentialsProviderChain()
    val stsProvider =
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
