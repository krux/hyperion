package com.krux.hyperion.aws

/*
 * This object includes the following fields from the Activity object.
 *
 * @param dependsOn One or more references to other Activities that must reach the FINISHED state before this activity will start.  Activity object reference No
 * @param onFail  The SnsAlarm to use when the current instance fails.  SnsAlarm object reference No
 * @param onSuccess The SnsAlarm to use when the current instance succeeds. SnsAlarm object reference No
 * @param precondition  A condition that must be met before the object can run. To specify multiple conditions, add multiple precondition fields. The activity cannot run until all its conditions are met. List of preconditions No
 *
 */

/*
 * This object includes the following fields from RunnableObject.
 *
 * @param attemptTimeout The timeout time interval for an object attempt. If an attempt does not complete within the start time plus this time interval, AWS Data Pipeline marks the attempt as failed and your retry settings determine the next steps taken.
 * @param failureAndRerunMode  Determines whether pipeline object failures and rerun commands cascade through pipeline object dependencies. For more information, see Cascading Failures and Reruns. String. Possible values are cascade and none.
 * @param lateAfterTimeout The time period in which the object run must start. If the object does not start within the scheduled start time plus this time interval, it is considered late.  Time period; for example, "1 hour". The minimum value is "15 minutes".  No
 * @param maximumRetries  The maximum number of times to retry the action. The default value is 2, which results in 3 tries total (1 original attempt plus 2 retries). The maximum value is 5 (6 total attempts). Integer
 * @param onFail An action to run when the current object fails. List of SnsAlarm object references  No
 * @param onLateAction  The SnsAlarm to use when the object's run is late.  List of SnsAlarm object references  No
 * @param onSuccess An action to run when the current object succeeds.  List of SnsAlarm object references  No
 * @param retryDelay The timeout duration between two retry attempts. The default is 10 minutes. Period. Minimum is "1 second".
 */

/*
 * This object includes the following fields from SchedulableObject
 * @param maxActiveInstances The maximum number of concurrent active instances of a component. For activities, setting this to 1 runs instances in strict chronological order.
 *                           A value greater than 1 allows different instances of the activity to run concurrently and requires you to ensure your activity can tolerate
 *                           concurrent execution. Integer between 1 and 5
 * @param runsOn  The computational resource to run the activity or command. For example, an Amazon EC2 instance or Amazon EMR cluster. Resource object reference
 * @param workerGroup The worker group. This is used for routing tasks. If you provide a runsOn value and workerGroup exists, workerGroup is ignored. String
 */


/**
 * AWS Data Pipeline activity objects.
 *
 * ref: http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-activities.html
 */
trait AdpActivity extends AdpDataPipelineObject {

  /**
   * The worker group. This is used for routing tasks.
   * If you provide a runsOn value and workerGroup exists, workerGroup is ignored.
   */
  def workerGroup: Option[String]

  /**
   * One or more references to other Activities that must reach the FINISHED state before this
   * activity will start.
   */
  def dependsOn: Option[Seq[AdpRef[AdpActivity]]]

  /**
   * A condition that must be met before the object can run. To specify multiple conditions,
   * add multiple precondition fields. The activity cannot run until all its conditions are met.
   */
  def precondition: Option[Seq[AdpRef[AdpPrecondition]]]

  /**
   * The SNS alarm to raise when the activity fails.
   */
  def onFail: Option[Seq[AdpRef[AdpSnsAlarm]]]

  /**
   * The SNS alarm to raise when the activity succeeds.
   */
  def onSuccess: Option[Seq[AdpRef[AdpSnsAlarm]]]

  /**
   * The SNS alarm to raise when the activity fails to start on time.
   */
  def onLateAction: Option[Seq[AdpRef[AdpSnsAlarm]]]

  /**
   * The timeout time interval for an object attempt. If an attempt does not complete within the start time
   * plus this time interval, AWS Data Pipeline marks the attempt as failed and your retry settings
   * determine the next steps taken.
   */
  def attemptTimeout: Option[String]

  /**
   * The time period in which the object run must start. If the object does not start within the scheduled
   * start time plus this time interval, it is considered late
   */
  def lateAfterTimeout: Option[String]

  /**
   * The maximum number of times to retry the action. The default value is 2, which results in 3 tries
   * total (1 original attempt plus 2 retries). The maximum value is 5 (6 total attempts).
   */
  def maximumRetries: Option[String]

  /**
   * The timeout duration between two retry attempts. The default is 10 minutes.
   */
  def retryDelay: Option[String]

  /**
   * Determines whether pipeline object failures and rerun commands cascade through pipeline object dependencies
   *
   * Possible values include cascade and none.
   */
  def failureAndRerunMode: Option[String]

  /**
   *The maximum number of concurrent active instances of a component. Re-runs do not count toward the number of active instances.
   */
  def maxActiveInstances: Option[String]
}

/**
 * ref: http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-copyactivity.html
 *
 * @param id The ID of the object. IDs must be unique within a pipeline definition.
 * @param name The optional, user-defined label of the object. If you do not provide a name for an object in a pipeline definition, AWS Data Pipeline automatically duplicates the value of id.
 * @param input The input data source.
 * @param output The location for the output.
 * @param dependsOn Required for AdpActivity
 *
 */
case class AdpCopyActivity (
  id: String,
  name: Option[String],
  input: AdpRef[AdpDataNode],
  output: AdpRef[AdpDataNode],
  workerGroup: Option[String],
  runsOn: Option[AdpRef[AdpEc2Resource]],
  dependsOn: Option[Seq[AdpRef[AdpActivity]]],
  precondition: Option[Seq[AdpRef[AdpPrecondition]]],
  onFail: Option[Seq[AdpRef[AdpSnsAlarm]]],
  onSuccess: Option[Seq[AdpRef[AdpSnsAlarm]]],
  onLateAction: Option[Seq[AdpRef[AdpSnsAlarm]]],
  attemptTimeout: Option[String],
  lateAfterTimeout: Option[String],
  maximumRetries: Option[String],
  retryDelay: Option[String],
  failureAndRerunMode: Option[String],
  maxActiveInstances: Option[String]
) extends AdpActivity {

  val `type` = "CopyActivity"

}

/**
 * ref: http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-redshiftcopyactivity.html
 *
 * @param id required for AdpDataPipelineObject
 * @param name required for AdpDataPipelineObject
 * @param input The input data node. The data source can be Amazon S3, DynamoDB, or Amazon Redshift.
 * @param insertMode Determines what AWS Data Pipeline does with pre-existing data in the target
 *   table that overlaps with rows in the data to be loaded. Valid values are KEEP_EXISTING,
 *   OVERWRITE_EXISTING, and TRUNCATE.
 * @param output The output data node. The output location can be Amazon S3 or Amazon Redshift.
 * @param runsOn Required for AdpActivity
 * @param transformSql The SQL SELECT expression used to transform the input data.
 * @param commandOptions Takes COPY parameters to pass to the Amazon Redshift data node.
 * @param queue Corresponds to the query_group setting in Amazon Redshift, which allows you to
 *   assign and prioritize concurrent activities based on their placement in queues. Amazon Redshift
 *   limits the number of simultaneous connections to 15.
 * @param dependsOn Required for AdpActivity
 */
case class AdpRedshiftCopyActivity (
  id: String,
  name: Option[String],
  insertMode: String,
  transformSql: Option[String],
  queue: Option[String],
  commandOptions: Option[Seq[String]],
  input: AdpRef[AdpDataNode],
  output: AdpRef[AdpDataNode],
  workerGroup: Option[String],
  runsOn: Option[AdpRef[AdpEc2Resource]],
  dependsOn: Option[Seq[AdpRef[AdpActivity]]],
  precondition: Option[Seq[AdpRef[AdpPrecondition]]],
  onFail: Option[Seq[AdpRef[AdpSnsAlarm]]],
  onSuccess: Option[Seq[AdpRef[AdpSnsAlarm]]],
  onLateAction: Option[Seq[AdpRef[AdpSnsAlarm]]],
  attemptTimeout: Option[String],
  lateAfterTimeout: Option[String],
  maximumRetries: Option[String],
  retryDelay: Option[String],
  failureAndRerunMode: Option[String],
  maxActiveInstances: Option[String]
) extends AdpActivity {

  val `type` = "RedshiftCopyActivity"

}

/**
 * Runs an Amazon EMR job.
 *
 * AWS Data Pipeline uses a different format for steps than Amazon EMR, for example AWS Data
 * Pipeline uses comma-separated arguments after the JAR name in the EmrActivity step field.
 *
 * @param input The input data source.
 * @param output The location for the output
 * @param preStepCommand Shell scripts to be run before any steps are run. To specify multiple scripts,
 *   up to 255, add multiple preStepCommand fields.
 * @param postStepCommand Shell scripts to be run after all steps are finished. To specify multiple
 *   scripts, up to 255, add multiple postStepCommand fields.
 * @param runsOn The Amazon EMR cluster to run this cluster.
 * @param step One or more steps for the cluster to run. To specify multiple steps, up to 255, add
 *   multiple step fields. Use comma-separated arguments after the JAR name; for example,
 *   "s3://example-bucket/MyWork.jar,arg1,arg2,arg3".
 */
case class AdpEmrActivity (
  id: String,
  name: Option[String],
  step: Seq[String],
  preStepCommand: Option[Seq[String]],
  postStepCommand: Option[Seq[String]],
  input: Option[Seq[AdpRef[AdpDataNode]]],
  output: Option[Seq[AdpRef[AdpDataNode]]],
  workerGroup: Option[String],
  runsOn: Option[AdpRef[AdpEmrCluster]],
  dependsOn: Option[Seq[AdpRef[AdpActivity]]],
  precondition: Option[Seq[AdpRef[AdpPrecondition]]],
  onFail: Option[Seq[AdpRef[AdpSnsAlarm]]],
  onSuccess: Option[Seq[AdpRef[AdpSnsAlarm]]],
  onLateAction: Option[Seq[AdpRef[AdpSnsAlarm]]],
  attemptTimeout: Option[String],
  lateAfterTimeout: Option[String],
  maximumRetries: Option[String],
  retryDelay: Option[String],
  failureAndRerunMode: Option[String],
  maxActiveInstances: Option[String]
) extends AdpActivity {

  val `type` = "EmrActivity"

}

class AdpHadoopActivity (
  val id: String,
  val name: Option[String],
  val jarUri: String,
  val mainClass: Option[String],
  val argument: Option[Seq[String]],
  val hadoopQueue: Option[String],
  val preActivityTaskConfig: Option[AdpRef[AdpShellScriptConfig]],
  val postActivityTaskConfig: Option[AdpRef[AdpShellScriptConfig]],
  val input: Option[Seq[AdpRef[AdpDataNode]]],
  val output: Option[Seq[AdpRef[AdpDataNode]]],
  val workerGroup: Option[String],
  val runsOn: Option[AdpRef[AdpEmrCluster]],
  val dependsOn: Option[Seq[AdpRef[AdpActivity]]],
  val precondition: Option[Seq[AdpRef[AdpPrecondition]]],
  val onFail: Option[Seq[AdpRef[AdpSnsAlarm]]],
  val onSuccess: Option[Seq[AdpRef[AdpSnsAlarm]]],
  val onLateAction: Option[Seq[AdpRef[AdpSnsAlarm]]],
  val attemptTimeout: Option[String],
  val lateAfterTimeout: Option[String],
  val maximumRetries: Option[String],
  val retryDelay: Option[String],
  val failureAndRerunMode: Option[String],
  val maxActiveInstances: Option[String]
  // XXX - no evidence this is supported actionOnResourceFailure: Option[String],
  // XXX - no evidence this is supported actionOnTaskFailure: Option[String]
) extends AdpActivity {

  val `type` = "HadoopActivity"

}

/**
 * ref: http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-hiveactivity.html
 *
 * @param hiveScript The Hive script to run.
 * @param input The input data source.  Data node object reference  Yes
 * @param output The location for the output.  Data node object reference  Yes
 * @param runsOn The Amazon EMR cluster to run this activity.  EmrCluster object reference Yes
 * @param scriptUri The location of the Hive script to run. For example, s3://script location.
 * @param scriptVariable Specifies script variables for Amazon EMR to pass to Hive while running a script.
 *                       For example, the following example script variables would pass a SAMPLE and
 *                       FILTER_DATE variable to Hive: SAMPLE=s3://elasticmapreduce/samples/hive-ads and
 *                       FILTER_DATE=#{format(@scheduledStartTime,'YYYY-MM-dd')}%
 *                       This field accepts multiple values and works with both script and scriptUri fields.
 *                       In addition, scriptVariable functions regardless of stage set to true or false.
 *                       This field is especially useful to send dynamic values to Hive using AWS Data
 *                       Pipeline expressions and functions. For more information, see Pipeline
 *                       Expressions and Functions.
 * @param stage Determines whether staging is enabled. Not permitted with Hive 11, so use an Amazon EMR AMI version 3.2.0 or greater.
 */
class AdpHiveActivity (
  val id: String,
  val name: Option[String],
  val hiveScript: Option[String],
  val scriptUri: Option[String],
  val scriptVariable: Option[Seq[String]],
  val stage: Option[String],
  val input: Option[Seq[AdpRef[AdpDataNode]]],
  val output: Option[Seq[AdpRef[AdpDataNode]]],
  val hadoopQueue: Option[String],
  val preActivityTaskConfig: Option[AdpRef[AdpShellScriptConfig]],
  val postActivityTaskConfig: Option[AdpRef[AdpShellScriptConfig]],
  val workerGroup: Option[String],
  val runsOn: Option[AdpRef[AdpEmrCluster]],
  val dependsOn: Option[Seq[AdpRef[AdpActivity]]],
  val precondition: Option[Seq[AdpRef[AdpPrecondition]]],
  val onFail: Option[Seq[AdpRef[AdpSnsAlarm]]],
  val onSuccess: Option[Seq[AdpRef[AdpSnsAlarm]]],
  val onLateAction: Option[Seq[AdpRef[AdpSnsAlarm]]],
  val attemptTimeout: Option[String],
  val lateAfterTimeout: Option[String],
  val maximumRetries: Option[String],
  val retryDelay: Option[String],
  val failureAndRerunMode: Option[String],
  val maxActiveInstances: Option[String]
) extends AdpActivity {

  val `type` = "HiveActivity"

}

/**
 * ref: http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-hivecopyactivity.html
 *
 * @param filterSql  A Hive SQL statement fragment that filters a subset of DynamoDB or Amazon S3 data to copy. The filter should only contain predicates and not begin with a WHERE clause, because AWS Data Pipeline adds it automatically.
 * @param generatedScriptsPath  An Amazon S3 path capturing the Hive script that ran after all the expressions in it were evaluated, including staging information. This script is stored for troubleshooting purposes.
 * @param input The input data node. This must be S3DataNode or DynamoDBDataNode. If you use DynamoDBDataNode, specify a DynamoDBExportDataFormat.
 * @param output  The output data node. If input is S3DataNode, this must be DynamoDBDataNode. Otherwise, this can be S3DataNode or DynamoDBDataNode. If you use DynamoDBDataNode, specify a DynamoDBExportDataFormat.
 *
 */
case class AdpHiveCopyActivity (
  id: String,
  name: Option[String],
  filterSql: Option[String],
  generatedScriptsPath: Option[String],
  input: Option[AdpRef[AdpDataNode]],
  output: Option[AdpRef[AdpDataNode]],
  preActivityTaskConfig: Option[AdpRef[AdpShellScriptConfig]],
  postActivityTaskConfig: Option[AdpRef[AdpShellScriptConfig]],
  workerGroup: Option[String],
  runsOn: Option[AdpRef[AdpEmrCluster]],
  dependsOn: Option[Seq[AdpRef[AdpActivity]]],
  precondition: Option[Seq[AdpRef[AdpPrecondition]]],
  onFail: Option[Seq[AdpRef[AdpSnsAlarm]]],
  onSuccess: Option[Seq[AdpRef[AdpSnsAlarm]]],
  onLateAction: Option[Seq[AdpRef[AdpSnsAlarm]]],
  attemptTimeout: Option[String],
  lateAfterTimeout: Option[String],
  maximumRetries: Option[String],
  retryDelay: Option[String],
  failureAndRerunMode: Option[String],
  maxActiveInstances: Option[String]
) extends AdpActivity {

  val `type` = "HiveCopyActivity"

}

/**
 * ref: http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-pigactivity.html
 *
 * @param generatedScriptsPath An Amazon S3 path to capture the Pig script that ran after all the expressions
 *                             in it were evaluated, including staging information. This script is stored for
 *                             historical, troubleshooting purposes.
 * @param input The input data source.
 * @param output The location for the output.
 * @param script The Pig script to run. You must specify either script or scriptUri.
 * @param scriptUri The location of the Pig script to run. For example, s3://script location. You must specify either scriptUri or script.
 * @param scriptVariable The arguments to pass to the Pig script. You can use scriptVariable with script or scriptUri.
 * @param stage Determines whether staging is enabled and allows your Pig script to have access to the
 *              staged-data tables, such as {{{\$\{INPUT1\}}}} and {{{\$\{OUTPUT1\}}}}.
 *
 */
class AdpPigActivity (
  val id: String,
  val name: Option[String],
  val script: Option[String],
  val scriptUri: Option[String],
  val scriptVariable: Option[Seq[String]],
  val generatedScriptsPath: Option[String],
  val stage: Option[String],
  val input: Option[AdpRef[AdpDataNode]],
  val output: Option[AdpRef[AdpDataNode]],
  val preActivityTaskConfig: Option[AdpRef[AdpShellScriptConfig]],
  val postActivityTaskConfig: Option[AdpRef[AdpShellScriptConfig]],
  val workerGroup: Option[String],
  val runsOn: Option[AdpRef[AdpEmrCluster]],
  val dependsOn: Option[Seq[AdpRef[AdpActivity]]],
  val precondition: Option[Seq[AdpRef[AdpPrecondition]]],
  val onFail: Option[Seq[AdpRef[AdpSnsAlarm]]],
  val onSuccess: Option[Seq[AdpRef[AdpSnsAlarm]]],
  val onLateAction: Option[Seq[AdpRef[AdpSnsAlarm]]],
  val attemptTimeout: Option[String],
  val lateAfterTimeout: Option[String],
  val maximumRetries: Option[String],
  val retryDelay: Option[String],
  val failureAndRerunMode: Option[String],
  val maxActiveInstances: Option[String]
) extends AdpActivity {

  val `type` = "PigActivity"

}

/**
 * Runs a SQL query on a database. You specify the input table where the SQL query is run and the
 * output table where the results are stored. If the output table doesn't exist, this operation
 * creates a new table with that name.
 *
 * @param script The SQL script to run. For example:
 *   {{{insert into output select * from input where lastModified in range (?, ?)}}}
 *   the script is not evaluated as an expression. In that situation, scriptArgument are useful
 *
 * @param scriptArgument a list of variables for the script
 *
 * @note that scriptUri is deliberately missing from this implementation, as there does not seem to
 * be any use case for now.
 */
case class AdpSqlActivity (
  id: String,
  name: Option[String],
  script: Option[String],
  scriptUri: Option[String],
  scriptArgument: Option[Seq[String]],
  database: AdpRef[AdpDatabase],
  queue: Option[String],
  workerGroup: Option[String],
  runsOn: Option[AdpRef[AdpEc2Resource]],
  dependsOn: Option[Seq[AdpRef[AdpActivity]]],
  precondition: Option[Seq[AdpRef[AdpPrecondition]]],
  onFail: Option[Seq[AdpRef[AdpSnsAlarm]]],
  onSuccess: Option[Seq[AdpRef[AdpSnsAlarm]]],
  onLateAction: Option[Seq[AdpRef[AdpSnsAlarm]]],
  attemptTimeout: Option[String],
  lateAfterTimeout: Option[String],
  maximumRetries: Option[String],
  retryDelay: Option[String],
  failureAndRerunMode: Option[String],
  maxActiveInstances: Option[String]
) extends AdpActivity {

  val `type` = "SqlActivity"

}

/**
 * Runs a command on an EC2 node.  You specify the input S3 location, output S3 location and the
 * script/command.
 *
 * @param command The command to run. This value and any associated parameters must function in the environment from which you are running the Task Runner.
 * @param input The input data source.
 * @param output The location for the output.
 * @param scriptArgument A list of arguments to pass to the shell script.
 * @param scriptUri An Amazon S3 URI path for a file to download and run as a shell command. Only one scriptUri or command field should be present. scriptUri cannot use parameters, use command instead.
 * @param stage Determines whether staging is enabled and allows your shell commands to have access to the staged-data variables, such as {{{\$\{INPUT1_STAGING_DIR\}}}} and {{{\$\{OUTPUT1_STAGING_DIR\}}}}.
 * @param stderr The path that receives redirected system error messages from the command. If you use the runsOn field, this must be an Amazon S3 path because of the transitory nature of the resource running your activity. However if you specify the workerGroup field, a local file path is permitted.
 * @param stdout The Amazon S3 path that receives redirected output from the command. If you use the runsOn field, this must be an Amazon S3 path because of the transitory nature of the resource running your activity. However if you specify the workerGroup field, a local file path is permitted.
 */
class AdpShellCommandActivity (
  val id: String,
  val name: Option[String],
  val command: Option[String],
  val scriptUri: Option[String],
  val scriptArgument: Option[Seq[String]],
  val stdout: Option[String],
  val stderr: Option[String],
  val stage: Option[String],
  val input: Option[Seq[AdpRef[AdpDataNode]]],
  val output: Option[Seq[AdpRef[AdpDataNode]]],
  val workerGroup: Option[String],
  val runsOn: Option[AdpRef[AdpEc2Resource]],
  val dependsOn: Option[Seq[AdpRef[AdpActivity]]],
  val precondition: Option[Seq[AdpRef[AdpPrecondition]]],
  val onFail: Option[Seq[AdpRef[AdpSnsAlarm]]],
  val onSuccess: Option[Seq[AdpRef[AdpSnsAlarm]]],
  val onLateAction: Option[Seq[AdpRef[AdpSnsAlarm]]],
  val attemptTimeout: Option[String],
  val lateAfterTimeout: Option[String],
  val maximumRetries: Option[String],
  val retryDelay: Option[String],
  val failureAndRerunMode: Option[String],
  val maxActiveInstances: Option[String]
) extends AdpActivity {

  val `type` = "ShellCommandActivity"

}
