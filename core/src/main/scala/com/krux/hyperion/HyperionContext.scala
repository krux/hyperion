package com.krux.hyperion

import scala.util.Try

import com.krux.hyperion.expression.Duration
import com.typesafe.config.{ Config, ConfigFactory }

/**
 * Basic configurations
 */
class HyperionContext(config: Config) {

  def this() = this(ConfigFactory.load)

  lazy val scriptUri = config.getString("hyperion.script.uri")
  lazy val logUri = Try(config.getString("hyperion.log.uri")).toOption

  //
  // Default values
  //
  lazy val failureRerunMode = config.getString("hyperion.failure_rerun_mode")
  lazy val role = config.getString("hyperion.role")
  lazy val resourceRole = config.getString("hyperion.resource.role")
  lazy val region = config.getString("hyperion.aws.region")
  lazy val keypair = Try(config.getString("hyperion.aws.keypair")).toOption

  //
  // EC2 default configuration
  //
  lazy val ec2Region = Try(config.getString("hyperion.aws.ec2.region")).toOption.getOrElse(region)
  lazy val ec2KeyPair = Try(config.getString("hyperion.aws.ec2.keypair")).toOption.orElse(keypair)
  lazy val ec2AvailabilityZone = Try(config.getString("hyperion.aws.ec2.availability_zone")).toOption
  lazy val ec2SubnetId = Try(config.getString("hyperion.aws.ec2.subnet")).toOption
  lazy val ec2Role = Try(config.getString("hyperion.aws.ec2.role")).toOption.getOrElse(role)
  lazy val ec2ResourceRole = Try(config.getString("hyperion.aws.ec2.resource.role")).toOption.getOrElse(resourceRole)

  lazy val ec2SecurityGroup = Try(config.getString("hyperion.aws.ec2.securitygroup")).toOption
  lazy val ec2SecurityGroupId = Try(config.getString("hyperion.aws.ec2.securitygroupid")).toOption
  lazy val ec2InstanceType = config.getString("hyperion.aws.ec2.instance.type")
  lazy val ec2ImageId = config.getString(s"hyperion.aws.ec2.image.$ec2Region")
  lazy val ec2TerminateAfter = Try(config.getString("hyperion.aws.ec2.terminate")).toOption.map(Duration(_))

  //
  // EMR default configuration
  //
  lazy val emrRegion = Try(config.getString("hyperion.aws.emr.region")).toOption.getOrElse(region)
  lazy val emrKeyPair = Try(config.getString("hyperion.aws.emr.keypair")).toOption.orElse(keypair)
  lazy val emrAvailabilityZone = Try(config.getString("hyperion.aws.emr.availability_zone")).toOption
  lazy val emrSubnetId = Try(config.getString("hyperion.aws.emr.subnet")).toOption
  lazy val emrRole = Try(config.getString("hyperion.aws.emr.role")).toOption.getOrElse(role)
  lazy val emrResourceRole = Try(config.getString("hyperion.aws.emr.resource.role")).toOption.getOrElse(resourceRole)

  lazy val emrAmiVersion = Try(config.getString("hyperion.aws.emr.ami.version")).toOption
  lazy val emrReleaseLabel = Try(config.getString("hyperion.aws.emr.release_label")).toOption
  lazy val emrInstanceType = config.getString("hyperion.aws.emr.instance.type")
  lazy val emrEnvironmentUri = Try(config.getString("hyperion.aws.emr.env.uri")).toOption
  lazy val emrTerminateAfter = Try(config.getString("hyperion.aws.emr.terminate")).toOption.map(Duration(_))
  lazy val emrSparkVersion = Try(config.getString("hyperion.aws.emr.spark.version")).toOption

  //
  // SNS default configuration
  //
  lazy val snsRole = Try(config.getString("hyperion.aws.sns.role")).toOption
  lazy val snsTopic = Try(config.getString("hyperion.aws.sns.topic")).toOption

  //
  // Graphviz styles
  //
  lazy val graphStyles = config.getConfig("hyperion.graphviz.styles")

  //
  // Aws client configuration
  //
  lazy val maxRetry = config.getInt("hyperion.aws.client.max_retry")

}
