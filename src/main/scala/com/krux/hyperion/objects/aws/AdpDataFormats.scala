package com.krux.hyperion.objects.aws

/**
 * Defines AWS Data Pipeline Data Formats
 *
 * ref: http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-dataformats.html
 */
trait AdpDataFormat extends AdpDataPipelineObject

/**
 * A comma-delimited data format where the column separator is a tab character and the record
 * separator is a newline character.
 *
 * @param column The structure of the data file. Use column names and data types separated by a
 * space. For example:
 * {{{
 * [ "Name STRING", "Score INT", "DateOfBirth TIMESTAMP" ]
 * }}}
 * You can omit the data type when using STRING, which is the default.
 * Valid data types: TINYINT, SMALLINT, INT, BIGINT, BOOLEAN, FLOAT, DOUBLE, STRING, TIMESTAMP
 *
 * @param escapeChar A character, for example "\", that instructs the parser to ignore the next
 * character.
 */
case class AdpTsvDataFormat(
  id: String,
  name: Option[String],
  column: Option[Seq[String]],
  escapeChar: Option[String]
) extends AdpDataFormat {

  val `type` = "TSV"

}

/**
 * CSV Data Format
 *
 * A comma-delimited data format where the column separator is a comma and the record separator is a newline character.
 */
case class AdpCsvDataFormat(
  id: String,
  name: Option[String],
  column: Option[Seq[String]],
  escapeChar: Option[String]
) extends AdpDataFormat {

  val `type` = "CSV"

}

/**
 * Custom Data Format
 *
 * A custom data format defined by a combination of a certain column separator, record separator, and escape character.
 */
case class AdpCustomDataFormat(
  id: String,
  name: Option[String],
  column: Option[Seq[String]],
  columnSeparator: String,
  recordSeparator: String
) extends AdpDataFormat {

  val `type` = "Custom"

}

/**
 * DynamoDBDataFormat
 *
 * Applies a schema to a DynamoDB table to make it accessible by a Hive query. DynamoDBDataFormat is used with a HiveActivity object and a DynamoDBDataNode input and output. DynamoDBDataFormat requires that you specify all columns in your Hive query. For more flexibility to specify certain columns in a Hive query or Amazon S3 support, see DynamoDBExportDataFormat.
 */
case class AdpDynamoDBDataFormat(
  id: String,
  name: Option[String],
  column: Option[Seq[String]]
) extends AdpDataFormat {

  val `type` = "DynamoDB"

}

/**
 * DynamoDBExportDataFormat
 *
 * Applies a schema to an DynamoDB table to make it accessible by a Hive query.
 * Use DynamoDBExportDataFormat with a HiveCopyActivity object and DynamoDBDataNode
 * or S3DataNode input and output. DynamoDBExportDataFormat has the following benefits:
 *
 * * Provides both DynamoDB and Amazon S3 support
 * * Allows you to filter data by certain columns in your Hive query
 * * Exports all attributes from DynamoDB even if you have a sparse schema
 */
case class AdpDynamoDBExportDataFormat(
  id: String,
  name: Option[String],
  column: Option[Seq[String]]
) extends AdpDataFormat {

  val `type` = "DynamoDBExport"

}

/**
 * RegEx Data Format
 *
 * A custom data format defined by a regular expression.
 * @param inputRegEx The regular expression to parse an S3 input file.
 *                   inputRegEx provides a way to retrieve columns from relatively unstructured data in a file.
 * @param outputFormat The column fields retrieved by inputRegEx, but referenced as %1, %2, %3, etc.
 *                     using Java formatter syntax.
 */
case class AdpRegExDataFormat(
  id: String,
  name: Option[String],
  column: Option[Seq[String]],
  inputRegEx: String,
  outputFormat: String
) extends AdpDataFormat {

  val `type` = "RegEx"

}
