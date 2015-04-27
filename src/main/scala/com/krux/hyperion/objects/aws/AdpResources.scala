package com.krux.hyperion.objects.aws

/**
 * Defines the AWS Data Pipeline Resources
 *
 * ref: http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-resources.html
 */
trait AdpResource extends AdpDataPipelineObject {
  def terminateAfter: String
  def keyPair: Option[String]
  def region: Option[String]
}


/**
 * An EC2 instance that will perform the work defined by a pipeline activity.
 *
 * @param terminateAfter The amount of time to wait before terminating the resource.
 * @param role The IAM role to use to create the EC2 instance.
 * @param resourceRole The IAM role to use to control the resources that the EC2 instance can access.
 * @param imageId The AMI version to use for the EC2 instances. For more information, see Amazon Machine Images (AMIs).
 * @param instanceType The type of EC2 instance to use for the resource pool. The default value is m1.small.
 * @param region A region code to specify that the resource should run in a different region.
 * @param securityGroups The names of one or more security groups to use for the instances in the resource pool.
 *                       By default, Amazon EC2 uses the default security group.
 * @param securityGroupIds The IDs of one or more security groups to use for the instances in the resource pool.
 *                         By default, Amazon EC2 uses the default security group.
 * @param associatePublicIpAddress Indicates whether to assign a public IP address to an instance.
 *                                 An instance in a VPC can't access Amazon S3 unless it has a public IP address or
 *                                 a network address translation (NAT) instance with proper routing configuration.
 *                                 If the instance is in EC2-Classic or a default VPC, the default value is true.
 *                                 Otherwise, the default value is false.
 * @param keyPair The name of the key pair. If you launch an EC2 instance without specifying a key pair, you
 *                can't log on to it.
 * @param subnetId The ID of the subnet to launch the instance into.
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
  keyPair: Option[String],
  subnetId: Option[String]
) extends AdpResource {

  val `type` = "Ec2Resource"

}

/**
 * Represents the configuration of an Amazon EMR cluster. This object is used by EmrActivity to
 * launch a cluster.
 *
 * @param bootstrapAction An action to run when the cluster starts. You can specify comma-separated arguments.
 *                        To specify multiple actions, up to 255, add multiple bootstrapAction fields.
 *                        The default behavior is to start the cluster without any bootstrap actions.
 * @param amiVersion The Amazon Machine Image (AMI) version to use by Amazon EMR to install the cluster nodes.
 * @param masterInstanceType The type of EC2 instance to use for the master node. The default value is m1.small.
 * @param coreInstanceType The type of EC2 instance to use for core nodes. The default value is m1.small.
 * @param coreInstanceCount The number of core nodes to use for the cluster. The default value is 1.
 * @param taskInstanceType The type of EC2 instance to use for task nodes.
 * @param taskInstanceCount The number of task nodes to use for the cluster. The default value is 1.
 * @param taskInstanceBidPrice The maximum dollar amount for your Spot Instance bid and is a decimal value
 *                             between 0 and 20.00 exclusive.
 *                             Setting this value enables Spot Instances for the EMR cluster task nodes.
 * @param terminateAfter The amount of time to wait before terminating the resource.
 * @param keyPair The Amazon EC2 key pair to use to log onto the master node of the cluster.
 *                The default action is not to attach a key pair to the cluster.
 * @param region A region code to specify that the resource should run in a different region.
 * @param enableDebugging Enables debugging on the Amazon EMR cluster.
 * @param supportedProducts A parameter that installs third-party software on an Amazon EMR cluster, for
 *                          example installing a third-party distribution of Hadoop.
 * @param subnetId The ID of the subnet to launch the instance into.
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
  taskInstanceBidPrice: Option[String],
  terminateAfter: String,
  keyPair: Option[String],
  region: Option[String],
  enableDebugging: Option[String],
  supportedProducts: Option[String],
  subnetId: Option[String]
) extends AdpResource {

  val `type` = "EmrCluster"

}
