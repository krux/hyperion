package com.krux.hyperion.client

import com.amazonaws.auth.{
  AWSSessionCredentialsProvider, BasicSessionCredentials, DefaultAWSCredentialsProviderChain,
  STSAssumeRoleSessionCredentialsProvider
}
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.datapipeline.DataPipelineClient
import org.slf4j.LoggerFactory

import com.krux.hyperion.DataPipelineDefGroup
import com.krux.stubborn.policy.ExponentialBackoffAndJitter
import com.krux.stubborn.Retryable


trait AwsClient extends Retryable with ExponentialBackoffAndJitter {

  lazy val log = LoggerFactory.getLogger(getClass)

  def client: DataPipelineClient

  override def base: Int = 3000

  override def cap: Int = 24000 // theoretical max retry delay with 3 retries

}

object AwsClient {

  def getClient(regionId: Option[String] = None, roleArn: Option[String] = None)
    : DataPipelineClient = {

    val region: Region =
      Region.getRegion(regionId.map(r => Regions.fromName(r)).getOrElse(Regions.US_EAST_1))

    lazy val defaultProvider = new DefaultAWSCredentialsProviderChain()

    lazy val stsProvider =
      roleArn.map(new STSAssumeRoleSessionCredentialsProvider(defaultProvider, _, "hyperion"))

    // In case AWS_SECURITY_TOKEN is set, use the session provider instead
    lazy val sessionCredentialsProvider =
      Option(System.getenv("AWS_SECURITY_TOKEN"))
        .map { token =>
          val sessionCredentials = new BasicSessionCredentials(
            defaultProvider.getCredentials().getAWSAccessKeyId(),
            defaultProvider.getCredentials().getAWSSecretKey(),
            token
          )
          new AWSSessionCredentialsProvider {
            def getCredentials() = sessionCredentials
            def refresh() = ()
          }
        }

    new DataPipelineClient(
      sessionCredentialsProvider
        .orElse(stsProvider)
        .getOrElse(defaultProvider)
    ).withRegion(region)
  }

  def apply(
      pipelineDef: DataPipelineDefGroup,
      regionId: Option[String],
      roleArn: Option[String]
    ): AwsClientForDef =
    new AwsClientForDef(getClient(regionId, roleArn), pipelineDef)

}
