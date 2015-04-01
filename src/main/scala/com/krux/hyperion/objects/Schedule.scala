package com.krux.hyperion.objects

import com.github.nscala_time.time.Imports.{DateTime, DateTimeZone}
import com.krux.hyperion.expressions.DpPeriod
import com.krux.hyperion.Implicits._
import com.krux.hyperion.objects.aws.AdpSchedule
import com.krux.hyperion.objects.ScheduleType._

/**
 * Cron liked schedule that runs at defined period.
 *
 * @note If start time given is a past time, data pipeline will perform back fill from the start.
 */
case class Schedule(
  id: String = "PipelineSchedule",
  // if None, will use first activation datetime
  start: Option[DateTime] = None,
  period: DpPeriod = 1.day,
  occurrences: Option[Int] = None,
  end: Option[DateTime] = None,
  scheduleType: ScheduleType = Cron
) extends PipelineObject {

  def startAtActivation = this.copy(start = None)

  def startDateTime(dt: DateTime) = this.copy(start = Option(dt))

  def startTodayAt(hourOfDay: Int, minuteOfHour: Int, secondOfMinute: Int) =
    startThisDayOfXAt(0, hourOfDay, minuteOfHour, secondOfMinute)((dt, i) => dt)

  def startThisWeekAt(dayOfWeek: Int, hourOfDay: Int, minuteOfHour: Int, secondOfMinute: Int) =
    startThisDayOfXAt(dayOfWeek, hourOfDay, minuteOfHour, secondOfMinute)(_.withDayOfWeek(_))

  def startThisMonthAt(dayOfMonth: Int, hourOfDay: Int, minuteOfHour: Int, secondOfMinute: Int) =
    startThisDayOfXAt(dayOfMonth, hourOfDay, minuteOfHour, secondOfMinute)(_.withDayOfMonth(_))

  private def startThisDayOfXAt(dayOfX: Int, hourOfDay: Int, minuteOfHour: Int,
      secondOfMinute: Int)(dayOfFunc: (DateTime, Int) => DateTime) = {
    val startDt = dayOfFunc(DateTime.now.withZone(DateTimeZone.UTC), dayOfX)
      .withTime(hourOfDay, minuteOfHour, secondOfMinute, 0)
    this.copy(start = Option(startDt))
  }

  def every(p: DpPeriod) = this.copy(period = p)

  @deprecated("Use 'every' instead of 'period'", "2015-04-01") def period(p: DpPeriod) = this.copy(period = p)

  def until(dt: DateTime) = this.copy(end = Option(dt))

  def stopAfter(occurrences: Int) = this.copy(occurrences = Some(occurrences))

  def serialize = start match {
    case Some(dt) =>
      AdpSchedule(
        id = id,
        name = Some(id),
        period = period.toString,
        startAt = None,
        startDateTime = Some(dt),
        endDateTime = end,
        occurrences = occurrences.map(_.toString)
      )

    case None =>
      AdpSchedule(
        id = id,
        name = Some(id),
        period = period.toString,
        startAt = Some("FIRST_ACTIVATION_DATE_TIME"),
        startDateTime = None,
        endDateTime = end,
        occurrences = occurrences.map(_.toString)
      )
  }

}

object Schedule {
  def cron = Schedule(scheduleType = Cron)

  def timeSeries = Schedule(scheduleType = TimeSeries)

  def onceAtActivation = Schedule(occurrences = Some(1), scheduleType = Cron)
}
