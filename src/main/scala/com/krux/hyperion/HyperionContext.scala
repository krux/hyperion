package com.krux.hyperion

import com.typesafe.config.{ConfigFactory, Config}
import scala.util.Try

/**
 * Basic configurations
 */
class HyperionContext(config: Config) {

  def this() = this(ConfigFactory.load)

  lazy val scriptUri = config.getString("hyperion.script.uri")
  lazy val logUri = config.getString("hyperion.log.uri")

  lazy val role = config.getString("hyperion.role")
  lazy val resourceRole = config.getString("hyperion.resource.role")

  lazy val failureRerunMode = config.getString("hyperion.failure_rerun_mode")

  lazy val keyPair = Try(config.getString("hyperion.aws.keypair")).toOption
  lazy val region = config.getString("hyperion.aws.region")
  lazy val accessKeyId = config.getString("hyperion.aws.access_key_id")
  lazy val accessKeySecret = config.getString("hyperion.aws.access_key_secret")
  lazy val subnetId = Try(config.getString("hyperion.aws.subnet")).toOption

  lazy val ec2SecurityGroup = config.getString("hyperion.aws.ec2.securitygroup")
  lazy val ec2InstanceType = config.getString("hyperion.aws.ec2.instance.type")
  lazy val ec2ImageId = config.getString(s"hyperion.aws.ec2.image.$region")
  lazy val ec2TerminateAfter = config.getString("hyperion.aws.ec2.terminate")

  lazy val emrAmiVersion = config.getString("hyperion.aws.emr.ami.version")
  lazy val emrInstanceType = config.getString("hyperion.aws.emr.instance.type")
  lazy val emrEnvironmentUri = Try(config.getString("hyperion.aws.emr.env.uri")).toOption
  lazy val emrTerminateAfter = config.getString("hyperion.aws.emr.terminate")
  lazy val sparkVersion = config.getString("hyperion.aws.emr.spark.version")

  lazy val snsRole = config.getString("hyperion.aws.sns.role")
  lazy val snsTopic = config.getString("hyperion.aws.sns.topic")

}
