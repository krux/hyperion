package com.krux.hyperion.objects.aws

/**
 * Defines the AWS Data Pipeline Resources
 *
 * ref: http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-resources.html
 */
trait AdpResource extends AdpDataPipelineObject {
  def terminateAfter: String
  def keyPair: Option[String]
}


/**
 * An EC2 instance that will perform the work defined by a pipeline activity.
 *
 * @param role The IAM role to use to create the EC2 instance.
 * @param resourceRole The IAM role to use to control the resources that the EC2 instance can access.
 * @param associatePublicIpAddress Indicates whether to assign a public IP address to an instance. (An instance in a VPC can't access Amazon S3 unless it has a public IP address or a network address translation (NAT) instance with proper routing configuration.) If the instance is in EC2-Classic or a default VPC, the default value is true. Otherwise, the default value is false.
 * @param imageId The AMI version to use for the EC2 instances. For more information, see Amazon Machine Images (AMIs).
 * @param instanceType The type of EC2 instance to use for the resource pool. The default value is m1.small. The values currently supported are: c1.medium, c1.xlarge, c3.2xlarge, c3.4xlarge, c3.8xlarge, c3.large, c3.xlarge, cc1.4xlarge, cc2.8xlarge, cg1.4xlarge, cr1.8xlarge, g2.2xlarge, hi1.4xlarge, hs1.8xlarge, i2.2xlarge, i2.4xlarge, i2.8xlarge, i2.xlarge, m1.large, m1.medium, m1.small, m1.xlarge, m2.2xlarge, m2.4xlarge, m2.xlarge, m3.2xlarge, m3.xlarge, t1.micro.
 * @param region A region code to specify that the resource should run in a different region. For more information, see Using a Pipeline with Resources in Multiple Regions.
 * @param securityGroups The names of one or more security groups to use for the instances in the resource pool. By default, Amazon EC2 uses the default security group.
 * @param securityGroupIds The IDs of one or more security groups to use for the instances in the resource pool. By default, Amazon EC2 uses the default security group.
 *
 */
case class AdpEc2Resource(
  id: String,
  name: Option[String],
  terminateAfter: String,
  role: Option[String],
  resourceRole: Option[String],
  imageId: Option[String],
  instanceType: Option[String],
  region: Option[String],
  securityGroups: Option[Seq[String]],
  securityGroupIds: Option[Seq[String]],
  associatePublicIpAddress: Option[String],
  keyPair: Option[String]
) extends AdpResource {

  val `type` = "Ec2Resource"

}


/**
 * Represents the configuration of an Amazon EMR cluster. This object is used by EmrActivity to
 * launch a cluster.
 */
case class AdpEmrCluster(
  id: String,
  name: Option[String],
  bootstrapAction: Seq[String],
  amiVersion: Option[String],
  masterInstanceType: Option[String],
  coreInstanceType: Option[String],
  coreInstanceCount: Option[String],
  taskInstanceType: Option[String],
  taskInstanceCount: Option[String],
  terminateAfter: String,
  keyPair: Option[String]
) extends AdpResource {

  val `type` = "EmrCluster"

}
