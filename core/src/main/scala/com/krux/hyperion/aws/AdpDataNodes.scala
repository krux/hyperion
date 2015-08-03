package com.krux.hyperion.aws

/**
 * AWS Data Pipeline DataNode objects
 *
 * ref: http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-datanodes.html
 *
 * @param precondition A list of precondition objects that must be true for the data node to be valid.
 *                     A data node cannot reach the READY status until all its conditions are met.
 *                     Preconditions do not have their own schedule or identity, instead they run on the
 *                     schedule of the activity or data node with which they are associated.
 * @param onFail The SnsAlarm to use when the current instance fails.
 * @param onSuccess The SnsAlarm to use when the current instance succeeds.
 */
trait AdpDataNode extends AdpDataPipelineObject {
  /**
   * A list of precondition objects that must be true for the data node to be valid.
   * A data node cannot reach the READY status until all its conditions are met.
   * Preconditions do not have their own schedule or identity, instead they run on the
   * schedule of the activity or data node with which they are associated.
   */
  def precondition: Option[Seq[AdpRef[AdpPrecondition]]]

  /**
   * The SNS alarm to raise when the current instance succeeds.
   */
  def onSuccess: Option[Seq[AdpRef[AdpSnsAlarm]]]

  /**
   * The SNS alarm to raise when the current instance fails.
   */
  def onFail: Option[Seq[AdpRef[AdpSnsAlarm]]]

}

/**
 * DynamoDB DataNode
 *
 * @param tableName The DynamoDB table.
 * @param region The AWS region where the DynamoDB table exists. It's used by HiveActivity when it performs staging for DynamoDB tables in Hive. For more information, see Using a Pipeline with Resources in Multiple Regions.
 * @param dynamoDBDataFormat Applies a schema to a DynamoDB table to make it accessible by a Hive query.
 * @param readThroughputPercent Sets the rate of read operations to keep your DynamoDB provisioned throughput rate in the allocated range for your table. The value is a double between .1 and 1.0, inclusively. For more information, see Specifying Read and Write Requirements for Tables.
 * @param writeThroughputPercent  Sets the rate of write operations to keep your DynamoDB provisioned throughput rate in the allocated range for your table. The value is a double between .1 and 1.0, inclusively. For more information, see Specifying Read and Write Requirements for Tables.
 */
case class AdpDynamoDBDataNode (
  id: String,
  name: Option[String],
  tableName: String,
  region: Option[String],
  dynamoDBDataFormat: Option[AdpRef[AdpDataFormat]],
  readThroughputPercent: Option[String],
  writeThroughputPercent: Option[String],
  precondition: Option[Seq[AdpRef[AdpPrecondition]]],
  onSuccess: Option[Seq[AdpRef[AdpSnsAlarm]]],
  onFail: Option[Seq[AdpRef[AdpSnsAlarm]]]
) extends AdpDataNode {

  val `type` = "DynamoDBDataNode"

}

/**
 * You must provide either a filePath or directoryPath value.
 */
case class AdpS3DataNode(
  id: String,
  name: Option[String],
  directoryPath: Option[String],
  filePath: Option[String],
  dataFormat: Option[AdpRef[AdpDataFormat]],
  manifestFilePath: Option[String],
  compression: Option[String],
  s3EncryptionType: Option[String],
  precondition: Option[Seq[AdpRef[AdpPrecondition]]],
  onSuccess: Option[Seq[AdpRef[AdpSnsAlarm]]],
  onFail: Option[Seq[AdpRef[AdpSnsAlarm]]]
) extends AdpDataNode {

  val `type` = "S3DataNode"

}

/**
 * Defines a data node using Amazon Redshift.
 *
 * @param primaryKeys If you do not specify primaryKeys for a destination table in
 * RedShiftCopyActivity, you can specify a list of columns using primaryKeys which will act as a
 * mergeKey. However, if you have an existing primaryKey defined in a Redshift table, this setting
 * overrides the existing key.
 */
case class AdpRedshiftDataNode(
  id: String,
  name: Option[String],
  createTableSql: Option[String],
  database: AdpRef[AdpRedshiftDatabase],
  schemaName: Option[String],
  tableName: String,
  primaryKeys: Option[Seq[String]],
  precondition: Option[Seq[AdpRef[AdpPrecondition]]],
  onSuccess: Option[Seq[AdpRef[AdpSnsAlarm]]],
  onFail: Option[Seq[AdpRef[AdpSnsAlarm]]]
) extends AdpDataNode {

  val `type` = "RedshiftDataNode"

}

/**
 * Example:
 * {{{
 * {
 *   "id" : "Sql Table",
 *   "type" : "MySqlDataNode",
 *   "schedule" : { "ref" : "CopyPeriod" },
 *   "table" : "adEvents",
 *   "selectQuery" : "select * from #{table} where eventTime >= '#{@scheduledStartTime.format('YYYY-MM-dd HH:mm:ss')}' and eventTime < '#{@scheduledEndTime.format('YYYY-MM-dd HH:mm:ss')}'"
 * }
 * }}}
 */
case class AdpSqlDataNode(
  id: String,
  name: Option[String],
  database: AdpRef[AdpDatabase],
  table: String,
  selectQuery: Option[String],
  insertQuery: Option[String],
  precondition: Option[Seq[AdpRef[AdpPrecondition]]],
  onSuccess: Option[Seq[AdpRef[AdpSnsAlarm]]],
  onFail: Option[Seq[AdpRef[AdpSnsAlarm]]]
) extends AdpDataNode {

  val `type` = "SqlDataNode"

}
