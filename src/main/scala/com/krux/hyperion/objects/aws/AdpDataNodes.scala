package com.krux.hyperion.objects.aws

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
  readThroughputPercent: Option[Double],
  writeThroughputPercent: Option[Double],
  precondition: Option[Seq[AdpRef[AdpPrecondition]]],
  onSuccess: Option[Seq[AdpRef[AdpSnsAlarm]]],
  onFail: Option[Seq[AdpRef[AdpSnsAlarm]]]
) extends AdpDataNode {

  val `type` = "DynamoDBDataNode"

}

/**
 * Defines a data node using Amazon S3.
 */
trait AdpS3DataNode extends AdpDataNode {

  /** The type of compression for the data described by the S3DataNode. none is no compression and
   * gzip is compressed with the gzip algorithm. This field is only supported for use with Amazon
   * Redshift and when you use S3DataNode with CopyActivit
   */
  def compression: Option[String]

  /** The format of the data described by the S3DataNode.
   */
  def dataFormat: Option[AdpRef[AdpDataFormat]]

  /** The Amazon S3 path to a manifest file in the format supported by Amazon Redshift. AWS Data
   * Pipeline uses the manifest file to copy the specified Amazon S3 files into the Amazon Redshift
   * table. This field is valid only when a RedshiftCopyActivity references the S3DataNode. For
   * more information, see Using a manifest to specify data files.
   */
  def manifestFilePath: Option[String]

  val `type` = "S3DataNode"

}

/** You must provide either a filePath or directoryPath value.
 */
case class AdpS3DirectoryDataNode(
  id: String,
  name: Option[String],
  compression: Option[String],
  dataFormat: Option[AdpRef[AdpDataFormat]],
  directoryPath: String,
  manifestFilePath: Option[String],
  precondition: Option[Seq[AdpRef[AdpPrecondition]]],
  onSuccess: Option[Seq[AdpRef[AdpSnsAlarm]]],
  onFail: Option[Seq[AdpRef[AdpSnsAlarm]]]
) extends AdpS3DataNode

/** You must provide either a filePath or directoryPath value.
 */
case class AdpS3FileDataNode(
  id: String,
  name: Option[String],
  compression: Option[String],
  dataFormat: Option[AdpRef[AdpDataFormat]],
  filePath: String,
  manifestFilePath: Option[String],
  precondition: Option[Seq[AdpRef[AdpPrecondition]]],
  onSuccess: Option[Seq[AdpRef[AdpSnsAlarm]]],
  onFail: Option[Seq[AdpRef[AdpSnsAlarm]]]
) extends AdpS3DataNode

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
 *   "username": "user_name",
 *   "*password": "my_password",
 *   "connectionString": "jdbc:mysql://mysqlinstance-rds.example.us-east-1.rds.amazonaws.com:3306/database_name",
 *   "selectQuery" : "select * from #{table} where eventTime >= '#{@scheduledStartTime.format('YYYY-MM-dd HH:mm:ss')}' and eventTime < '#{@scheduledEndTime.format('YYYY-MM-dd HH:mm:ss')}'"
 * }
 * }}}
 */
case class AdpSqlDataNode(
  id: String,
  name: Option[String],
  table: String,
  username: String,
  `*password`: String,
  connectionString: String,
  selectQuery: Option[String],
  insertQuery: Option[String],
  precondition: Option[Seq[AdpRef[AdpPrecondition]]],
  onSuccess: Option[Seq[AdpRef[AdpSnsAlarm]]],
  onFail: Option[Seq[AdpRef[AdpSnsAlarm]]]
) extends AdpDataNode {

  val `type` = "SqlDataNode"

}
