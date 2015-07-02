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

  lazy val failureRerunMode = config.getString("hyperion.failure_rerun_mode")
  lazy val role = config.getString("hyperion.role")
  lazy val resourceRole = config.getString("hyperion.resource.role")

  // EC2 default configuration
  lazy val ec2Region = config.getString("hyperion.aws.ec2.region")
  lazy val ec2KeyPair = Try(config.getString("hyperion.aws.ec2.keypair")).toOption
  lazy val ec2AvailabilityZone = Try(config.getString("hyperion.aws.ec2.availability_zone")).toOption
  lazy val ec2SubnetId = Try(config.getString("hyperion.aws.ec2.subnet")).toOption
  lazy val ec2Role = config.getString("hyperion.aws.ec2.role")
  lazy val ec2ResourceRole = config.getString("hyperion.aws.ec2.resource.role")

  lazy val ec2SecurityGroup = config.getString("hyperion.aws.ec2.securitygroup")
  lazy val ec2InstanceType = config.getString("hyperion.aws.ec2.instance.type")
  lazy val ec2ImageId = config.getString(s"hyperion.aws.ec2.image.$ec2Region")
  lazy val ec2TerminateAfter = config.getString("hyperion.aws.ec2.terminate")

  // EMR default configuration
  lazy val emrRegion = config.getString("hyperion.aws.emr.region")
  lazy val emrKeyPair = Try(config.getString("hyperion.aws.emr.keypair")).toOption
  lazy val emrAvailabilityZone = Try(config.getString("hyperion.aws.emr.availability_zone")).toOption
  lazy val emrSubnetId = Try(config.getString("hyperion.aws.emr.subnet")).toOption
  lazy val emrRole = config.getString("hyperion.aws.emr.role")
  lazy val emrResourceRole = config.getString("hyperion.aws.emr.resource.role")

  lazy val emrAmiVersion = config.getString("hyperion.aws.emr.ami.version")
  lazy val emrInstanceType = config.getString("hyperion.aws.emr.instance.type")
  lazy val emrEnvironmentUri = Try(config.getString("hyperion.aws.emr.env.uri")).toOption
  lazy val emrTerminateAfter = config.getString("hyperion.aws.emr.terminate")
  lazy val emrSparkVersion = config.getString("hyperion.aws.emr.spark.version")

  lazy val snsRole = Try(config.getString("hyperion.aws.sns.role")).toOption
  lazy val snsTopic = Try(config.getString("hyperion.aws.sns.topic")).toOption

}
