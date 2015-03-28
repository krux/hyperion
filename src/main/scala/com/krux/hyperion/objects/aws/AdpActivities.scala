package com.krux.hyperion.objects.aws

/**
 * AWS Data Pipeline activity objects.
 *
 * ref: http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-activities.html
 */
trait AdpActivity extends AdpDataPipelineObject {

  /**
   * One or more references to other Activities that must reach the FINISHED state before this
   * activity will start.
   */
  def dependsOn: Option[Seq[AdpRef[AdpActivity]]]

  /**
   * The computational resource to run the activity or command. For example, an Amazon EC2 instance
   * or Amazon EMR cluster.
   */
  def runsOn: AdpRef[AdpResource]
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
    input: AdpRef[AdpDataNode],
    insertMode: String,
    output: AdpRef[AdpDataNode],
    runsOn: AdpRef[AdpEc2Resource],
    transformSql: Option[String],
    commandOptions: Option[Seq[String]],
    queue: Option[String],
    dependsOn: Option[Seq[AdpRef[AdpActivity]]]
  ) extends AdpActivity {
  val `type` = "RedshiftCopyActivity"
}

/**
 * ref: http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-copyactivity.html
 *
 * Copies data from one location to another. CopyActivity supports S3DataNode and MySqlDataNode as
 * input and output and the copy operation is normally performed record-by-record. However,
 * CopyActivity provides a high-performance Amazon S3 to Amazon S3 copy when all the following
 * conditions are met:
 *
 * - The input and output are S3DataNodes
 *
 * - The dataFormat field is the same for input and output
 *
 */
case class AdpCopyActivity (
    id: String,
    name: Option[String],
    input: AdpRef[AdpDataNode],
    output: AdpRef[AdpDataNode],
    runsOn: AdpRef[AdpEc2Resource],
    dependsOn: Option[Seq[AdpRef[AdpActivity]]]
  ) extends AdpActivity {
  val `type` = "CopyActivity"
}

/**
 * Runs an Amazon EMR cluster.
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
    input: Option[AdpRef[AdpDataNode]],
    output: Option[AdpRef[AdpDataNode]],
    preStepCommand: Option[Seq[String]],
    postStepCommand: Option[Seq[String]],
    runsOn: AdpRef[AdpEmrCluster],
    step: Seq[String],
    dependsOn: Option[Seq[AdpRef[AdpActivity]]]
  ) extends AdpActivity {
  val `type` = "EmrActivity"
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
 * @param sriptArgument a list of variables for the script
 *
 * @note that scriptUri is deliberately missing from this implementation, as there does not seem to
 * be any use case for now.
 */
case class AdpSqlActivity (
    id: String,
    name: Option[String],
    database: AdpRef[AdpDatabase],
    script: String,
    scriptArgument: Option[Seq[String]],
    queue: Option[String],
    dependsOn: Option[Seq[AdpRef[AdpActivity]]],
    runsOn: AdpRef[AdpEc2Resource]
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
case class AdpShellCommandActivity (
    id: String,
    name: Option[String],
    command: Option[String],
    scriptUri: Option[String],
    scriptArgument: Option[Seq[String]],
    input: Option[AdpRef[AdpDataNode]],
    output: Option[AdpRef[AdpDataNode]],
    stage: String,
    stdout: Option[String],
    stderr: Option[String],
    dependsOn: Option[Seq[AdpRef[AdpActivity]]],
    runsOn: AdpRef[AdpEc2Resource]
  ) extends AdpActivity {
  val `type` = "ShellCommandActivity"
}
