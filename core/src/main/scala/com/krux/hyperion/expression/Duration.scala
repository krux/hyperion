package com.krux.hyperion.expression

/**
 * Indicates how often a scheduled event should run. It's expressed in the format "N
 * [years|months|weeks|days|hours|minutes]", where N is a positive integer value.
 *
 * The minimum period is 15 minutes and the maximum period is 3 years.
 */
sealed trait Duration {
  def n: Int
  def unit: String

  override def toString: String = s"$n $unit"
}

case class Year(n: Int) extends Duration {
  val unit = "years"

  require(0 < n && n <= 3, "Years must be between 1 and 3")
}

case class Month(n: Int) extends Duration {
  val unit = "months"

  require(0 < n && n <= 36, "Months must be between 1 and 36")
}

case class Week(n: Int) extends Duration {
  val unit = "weeks"

  require(0 < n && n <= 156, "Weeks must be between 1 and 156")
}

case class Day(n: Int) extends Duration {
  val unit = "days"

  require(0 < n && n <= 1095, "Days must be between 1 and 1095")
}

case class Hour(n: Int) extends Duration {
  val unit = "hours"

  require(0 < n && n <= 26280, "Hours must be between 1 and 26280")
}

case class Minute(n: Int) extends Duration {
  val unit = "minutes"

  require(10 <= n && n <= 1576800, "Minutes must be between 10 and 1576800")
}

/**
 * All supported data pipeline period units
 */
object Duration {

  def apply(s: String): Duration = {
    s.trim.toLowerCase.split(' ').toList match {
      case amount :: unit :: Nil => unit match {
        case "year"   | "years"   => Year(amount.toInt)
        case "month"  | "months"  => Month(amount.toInt)
        case "week"   | "weeks"   => Week(amount.toInt)
        case "day"    | "days"    => Day(amount.toInt)
        case "hour"   | "hours"   => Hour(amount.toInt)
        case "minute" | "minutes" => Minute(amount.toInt)
        case _ => throw new NumberFormatException(s"Cannot parse $s as a time period - $unit is not recognized")
      }

      case amount :: Nil => Hour(amount.toInt)
      case _ => throw new NumberFormatException(s"Cannot parse $s as a time period")
    }
  }
}
