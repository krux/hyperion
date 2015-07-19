package com.krux.hyperion.expression

/**
 * This implements the aws datapipeline Date and Time Functions
 *
 * ref: http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-pipeline-reference-functions-datetime.html
 */
trait DateTimeFunctions {

  def format(myDateTime: DateTimeExp, myFormat: String) =
    StringExp(s"format(${myDateTime.content},'$myFormat')")

  private def genDateTimeSubFunc(funcName: String, args: String*) =
    new DateTimeExp(s"$funcName(${args.mkString(",")})")

  def minusDays(myDateTime: DateTimeExp, daysToSub: Int) =
    genDateTimeSubFunc("minusDays", myDateTime.content, daysToSub.toString)

  def minusHours(myDateTime: DateTimeExp, daysToSub: Int) =
    genDateTimeSubFunc("minusHours", myDateTime.content, daysToSub.toString)

  def minusMinutes(myDateTime: DateTimeExp, daysToSub: Int) =
    genDateTimeSubFunc("minusMinutes", myDateTime.content, daysToSub.toString)

  def minusMonths(myDateTime: DateTimeExp, daysToSub: Int) =
    genDateTimeSubFunc("minusMonths", myDateTime.content, daysToSub.toString)

  def minusWeeks(myDateTime: DateTimeExp, daysToSub: Int) =
    genDateTimeSubFunc("minusWeeks", myDateTime.content, daysToSub.toString)

  def minusYears(myDateTime: DateTimeExp, daysToSub: Int) =
    genDateTimeSubFunc("minusYears", myDateTime.content, daysToSub.toString)

}

object DateTimeFunctions extends DateTimeFunctions
