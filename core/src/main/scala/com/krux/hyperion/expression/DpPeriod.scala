package com.krux.hyperion.expression

/**
 * All supported data pipeline period units
 */
object PeriodUnit extends Enumeration {

  type PeriodUnit = Value

  val Year = Value("years")
  val Month = Value("months")
  val Week = Value("weeks")
  val Day = Value("days")
  val Hour = Value("hours")
  val Minute = Value("minutes")

}


/**
 * Indicates how often a scheduled event should run. It's expressed in the format "N
 * [years|months|weeks|days|hours|minutes]", where N is a positive integer value.
 *
 * The minimum period is 15 minutes and the maximum period is 3 years.
 */
case class DpPeriod(n: Int, unit: PeriodUnit.PeriodUnit) {

  assert(
    unit match {
      case PeriodUnit.Minute => n > 15
      case PeriodUnit.Year => n > 0 && n <= 3
      case _ => n > 0
    }
  )

  override def toString = s"$n $unit"
}

/**
 * Builds DpPeriod, this is mainly used for using implicit conversions
 */
class DpPeriodBuilder(n: Int) {

  import PeriodUnit._

  def year = DpPeriod(n, Year)
  def years = this.year

  def month = DpPeriod(n, Month)
  def months = this.month

  def week = DpPeriod(n, Week)
  def weeks = this.week

  def day = DpPeriod(n, Day)
  def days = this.day

  def hour = DpPeriod(n, Hour)
  def hours = this.hour

}
