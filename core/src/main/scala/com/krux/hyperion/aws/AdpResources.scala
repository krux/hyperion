package com.krux.hyperion.aws

/**
 * Defines the AWS Data Pipeline Resources
 *
 * ref: http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-resources.html
 */
trait AdpResource extends AdpDataPipelineObject {
  /**
   * The amount of time to wait before terminating the resource.
   */
  def terminateAfter: Option[String]

  /**
   * The name of the key pair. If you launch an EC2 instance without specifying a key pair, you can't log on to it.
   */
  def keyPair: Option[String]

  /**
   * The code for the region that the EC2 instance should run in. By default, the instance runs in the same region
   * as the pipeline. You can run the instance in the same region as a dependent data set.
   */
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
 * @param availabilityZone The Availability Zone in which to launch the EC2 instance.
 * @param spotBidPrice The Spot Instance bid price for Ec2Resources. The maximum dollar amount for your Spot Instance bid and is a decimal value between 0 and 20.00 exclusive
 * @param useOnDemandOnLastAttempt On the last attempt to request a resource, this option will make a request for On-Demand Instances rather than Spot. This ensures that if all previous attempts have failed that the last attempt is not interrupted in the middle by changes in the spot market. Default value is True.
 * @param actionOnResourceFailure	Action to take when the resource fails.
 * @param actionOnTaskFailure	Action to take when the task associated with this resource fails.
 * @param maximumRetries Maximum number attempt retries on failure.
 */
case class AdpEc2Resource(
  id: String,
  name: Option[String],
  instanceType: Option[String],
  imageId: Option[String],
  role: Option[String],
  resourceRole: Option[String],
  runAsUser: Option[String],
  keyPair: Option[String],
  region: Option[String],
  availabilityZone: Option[String],
  subnetId: Option[String],
  associatePublicIpAddress: Option[String],
  securityGroups: Option[Seq[String]],
  securityGroupIds: Option[Seq[String]],
  spotBidPrice: Option[String],
  useOnDemandOnLastAttempt: Option[String],
  initTimeout: Option[String],
  terminateAfter: Option[String],
  actionOnResourceFailure: Option[String],
  actionOnTaskFailure: Option[String],
  httpProxy: Option[AdpRef[AdpHttpProxy]],
  maximumRetries: Option[String]
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
 * @param maximumRetries Maximum number attempt retries on failure.
 */
class AdpEmrCluster(
  val id: String,
  val name: Option[String],
  val amiVersion: Option[String],
  val supportedProducts: Option[String],
  val bootstrapAction: Seq[String],
  val enableDebugging: Option[String],
  val hadoopSchedulerType: Option[String],
  val keyPair: Option[String],
  val masterInstanceBidPrice: Option[String],
  val masterInstanceType: Option[String],
  val masterEbsConfiguration: Option[AdpRef[AdpEmrEbsConfiguration]],
  val coreInstanceBidPrice: Option[String],
  val coreInstanceCount: Option[String],
  val coreInstanceType: Option[String],
  val coreEbsConfiguration: Option[AdpRef[AdpEmrEbsConfiguration]],
  val taskInstanceBidPrice: Option[String],
  val taskInstanceCount: Option[String],
  val taskInstanceType: Option[String],
  val taskEbsConfiguration: Option[AdpRef[AdpEmrEbsConfiguration]],
  val region: Option[String],
  val availabilityZone: Option[String],
  val resourceRole: Option[String],
  val role: Option[String],
  val subnetId: Option[String],
  val emrManagedMasterSecurityGroupId: Option[String],
  val additionalMasterSecurityGroupIds: Option[Seq[String]],
  val emrManagedSlaveSecurityGroupId: Option[String],
  val additionalSlaveSecurityGroupIds: Option[Seq[String]],
  val useOnDemandOnLastAttempt: Option[String],
  val visibleToAllUsers: Option[String],
  val initTimeout: Option[String],
  val terminateAfter: Option[String],
  val actionOnResourceFailure: Option[String],
  val actionOnTaskFailure: Option[String],
  val httpProxy: Option[AdpRef[AdpHttpProxy]],
  val releaseLabel: Option[String],
  val applications: Option[Seq[String]],
  val configuration: Option[Seq[AdpRef[AdpEmrConfiguration]]],
  val maximumRetries: Option[String]
) extends AdpResource {

  val `type` = "EmrCluster"

}

case class AdpProperty(
  id: String,
  name: Option[String],
  key: Option[String],
  value: Option[String]
) extends AdpDataPipelineObject {

  val `type` = "Property"

}

case class AdpEmrConfiguration(
  id: String,
  name: Option[String],
  classification: Option[String],
  property: Option[Seq[AdpRef[AdpProperty]]],
  configuration: Option[Seq[AdpRef[AdpEmrConfiguration]]]
) extends AdpDataPipelineObject {

  val `type` = "EmrConfiguration"

}

case class AdpEmrEbsConfiguration(
  id: String,
  name: Option[String],
  ebsOptimized: Option[String],
  ebsBlockDeviceConfig: Option[AdpRef[AdpEmrEbsBlockDeviceConfig]]
) extends AdpDataPipelineObject {

  val `type` = "EbsConfiguration"

}

case class AdpEmrEbsBlockDeviceConfig(
  id: String,
  name: Option[String],
  volumesPerInstance: Option[String],
  volumeSpecification: Option[AdpRef[AdpEmrVolumeSpecification]]
)extends AdpDataPipelineObject {

  val `type` = "EbsBlockDeviceConfig"

}

case class AdpEmrVolumeSpecification(
  id: String,
  name: Option[String],
  sizeInGB: Option[String],
  volumeType: Option[String],
  iops: Option[String]
)extends AdpDataPipelineObject {

  val `type` = "VolumeSpecification"

}