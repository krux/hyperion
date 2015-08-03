package com.krux.hyperion.aws

/**
 * A condition that must be met before the object can run.
 * The activity cannot run until all its conditions are met.
 */
trait AdpPrecondition extends AdpDataPipelineObject {

  /**
   * The IAM role to use for this precondition.
   */
  def role: String

  /**
   * The precondition will be retried until the retryTimeout with a gap of retryDelay between attempts.
   * Time period; for example, "1 hour".
   */
  def preconditionTimeout: Option[String]
}

/**
 * A precondition to check that data exists in a DynamoDB table.
 *
 * @param tableName The DynamoDB table to check.
 */
case class AdpDynamoDBDataExistsPrecondition (
  id: String,
  name: Option[String],
  tableName: String,
  role: String,
  preconditionTimeout: Option[String]
) extends AdpPrecondition {

  val `type` = "DynamoDBDataExists"

}

/**
 * A precondition to check that the DynamoDB table exists.
 *
 * @param tableName The DynamoDB table to check.
 */
case class AdpDynamoDBTableExistsPrecondition(
  id: String,
  name: Option[String],
  tableName: String,
  role: String,
  preconditionTimeout: Option[String]
) extends AdpPrecondition {

  val `type` = "DynamoDBTableExists"

}

/**
 * Checks whether a data node object exists.
 */
case class AdpExistsPrecondition(
  id: String,
  name: Option[String],
  role: String,
  preconditionTimeout: Option[String]
) extends AdpPrecondition {

  val `type` = "Exists"

}

/**
 * Checks whether a key exists in an Amazon S3 data node.
 *
 * @param s3Key Amazon S3 key to check for existence.
 */
case class AdpS3KeyExistsPrecondition(
  id: String,
  name: Option[String],
  s3Key: String,
  role: String,
  preconditionTimeout: Option[String]
) extends AdpPrecondition {

  val `type` = "S3KeyExists"

}

/**
 * A precondition to check that the Amazon S3 objects with the given prefix (represented as a URI) are present.
 *
 * @param s3Prefix  The Amazon S3 prefix to check for existence of objects.
 */
case class AdpS3PrefixNotEmptyPrecondition(
  id: String,
  name: Option[String],
  s3Prefix: String,
  role: String,
  preconditionTimeout: Option[String]
) extends AdpPrecondition {

  val `type` = "S3PrefixNotEmpty"

}

/**
 * A Unix/Linux shell command that can be run as a precondition.
 *
 * @param command The command to run. This value and any associated parameters must function in the environment from which you are running the Task Runner.
 * @param scriptArgument A list of arguments to pass to the shell script.
 * @param scriptUri An Amazon S3 URI path for a file to download and run as a shell command. Only one scriptUri or command field should be present. scriptUri cannot use parameters, use command instead.
 * @param stdout The Amazon S3 path that receives redirected output from the command. If you use the runsOn field, this must be an Amazon S3 path because of the transitory nature of the resource running your activity. However if you specify the workerGroup field, a local file path is permitted.
 * @param stderr The Amazon S3 path that receives redirected system error messages from the command. If you use the runsOn field, this must be an Amazon S3 path because of the transitory nature of the resource running your activity. However if you specify the workerGroup field, a local file path is permitted.
 *
 */
case class AdpShellCommandPrecondition(
  id: String,
  name: Option[String],
  command: Option[String],
  scriptUri: Option[String],
  scriptArgument: Option[Seq[String]],
  stdout: Option[String],
  stderr: Option[String],
  role: String,
  preconditionTimeout: Option[String]
) extends AdpPrecondition {

  val `type` = "ShellCommandPrecondition"

}
