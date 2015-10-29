package com.krux.hyperion

import com.github.nscala_time.time.Imports.{DateTime, DateTimeZone}
import com.krux.hyperion.Implicits._
import com.krux.hyperion.aws.{AdpRef, AdpSchedule}
import com.krux.hyperion.common.{ScheduleObjectId, PipelineObjectId, PipelineObject}
import com.krux.hyperion.expression.Duration

/**
 * Schedule that runs at defined period.
 *
 * @note If start time given is a past time, data pipeline will perform back fill from the start.
 */
case class Schedule private (
  id: PipelineObjectId = ScheduleObjectId,
  // if None, will use first activation datetime
  start: Option[DateTime] = None,
  period: Duration = 1.day,
  end: Option[Either[Int, DateTime]] = None,
  scheduleType: ScheduleType = Cron
) extends PipelineObject {

  def startAtActivation = this.copy(start = None)

  def startDateTime(dt: DateTime) = this.copy(start = Option(dt))

  def startTodayAt(hourOfDay: Int, minuteOfHour: Int, secondOfMinute: Int) =
    startThisDayOfXAt(0, hourOfDay, minuteOfHour, secondOfMinute)((dt, _) => dt)

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

  def every(p: Duration) = this.copy(period = p)
  def until(dt: DateTime) = this.copy(end = Option(Right(dt)))
  def stopAfter(occurrences: Int) = this.copy(end = Option(Left(occurrences)))

  def objects: Iterable[PipelineObject] = None

  lazy val serialize: AdpSchedule = start match {
    case Some(dt) =>
      AdpSchedule(
        id = id,
        name = id.toOption,
        period = period.toString,
        startAt = None,
        startDateTime = Option(dt),
        endDateTime = end.flatMap {
          case Right(d) => Option(d)
          case _ => None
        },
        occurrences = end.flatMap {
          case Left(occurrences) => Option(occurrences.toString)
          case _ => None
        }
      )

    case None =>
      AdpSchedule(
        id = id,
        name = id.toOption,
        period = period.toString,
        startAt = Option("FIRST_ACTIVATION_DATE_TIME"),
        startDateTime = None,
        endDateTime = end.flatMap {
          case Right(dt) => Option(dt)
          case _ => None
        },
        occurrences = end.flatMap {
          case Left(occurrences) => Option(occurrences.toString)
          case _ => None
        }
      )
  }

  def ref: AdpRef[AdpSchedule] = AdpRef(serialize)

}

object Schedule {
  def cron = Schedule(scheduleType = Cron)

  def timeSeries = Schedule(scheduleType = TimeSeries)

  def onceAtActivation = Schedule(end = Option(Left(1)), scheduleType = Cron)
}
