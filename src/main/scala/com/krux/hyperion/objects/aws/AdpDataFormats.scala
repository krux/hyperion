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


case class AdpCsvDataFormat(
  id: String,
  name: Option[String],
  column: Option[Seq[String]],
  escapeChar: Option[String]
) extends AdpDataFormat {

  val `type` = "CSV"

}
