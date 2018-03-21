package com.krux.hyperion

import com.github.nscala_time.time.Imports.{DateTime, DateTimeZone, Period}

import com.krux.hyperion.aws.{AdpOnDemandSchedule, AdpRecurringSchedule, AdpRef}
import com.krux.hyperion.common.{PipelineObject, PipelineObjectId, ScheduleObjectId}
import com.krux.hyperion.expression.Duration
import com.krux.hyperion.Implicits._

/**
 * Schedule defines how a pipeline is run.
 */
sealed trait Schedule extends PipelineObject {
  def scheduleType: ScheduleType
}

/**
 * Schedule that runs on-demand.
 */
case object OnDemandSchedule extends Schedule {

  val id: PipelineObjectId = ScheduleObjectId
  val scheduleType: ScheduleType = OnDemand

  def objects: Iterable[PipelineObject] = None

  lazy val serialize: AdpOnDemandSchedule = new AdpOnDemandSchedule(
    id = id,
    name = id.toOption
  )

  def ref: AdpRef[AdpOnDemandSchedule] = AdpRef(serialize)

}

/**
 * Schedule that runs at defined period.
 *
 * @note If start time given is a past time, data pipeline will perform back fill from the start.
 */
final case class RecurringSchedule private[hyperion] (
  id: PipelineObjectId = ScheduleObjectId,
  // if None, will use first activation datetime
  start: Option[DateTime] = None,
  period: Duration = 1.day,
  end: Option[Either[Int, DateTime]] = None,
  scheduleType: ScheduleType = Cron
) extends Schedule {

  def startAtActivation = copy(start = None)

  def startDateTime(dt: DateTime) = copy(start = Option(dt))

  def startThisHourAt(minuteOfHour: Int, secondOfMinute: Int) = {
    val currentHour = DateTime.now.withZone(DateTimeZone.UTC).getHourOfDay
    startTodayAt(currentHour, minuteOfHour, secondOfMinute)
  }

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
    copy(start = Option(startDt))
  }

  def every(p: Duration) = this.copy(period = p)
  def until(dt: DateTime) = this.copy(end = Option(Right(dt)))
  def stopAfter(occurrences: Int) = this.copy(end = if (occurrences > 0) Option(Left(occurrences)) else None)

  def objects: Iterable[PipelineObject] = None

  lazy val serialize: AdpRecurringSchedule = start match {
    case Some(dt) =>
      AdpRecurringSchedule(
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
      AdpRecurringSchedule(
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

  def ref: AdpRef[AdpRecurringSchedule] = AdpRef(serialize)

}

object Schedule {

  def ondemand: OnDemandSchedule.type = OnDemandSchedule

  def cron: RecurringSchedule = RecurringSchedule(scheduleType = Cron)

  def timeSeries: RecurringSchedule = RecurringSchedule(scheduleType = TimeSeries)

  def onceAtActivation: RecurringSchedule = RecurringSchedule(end = Option(Left(1)), scheduleType = Cron)

  def delay(schedule: Schedule, by: Duration, multiplier: Int): Schedule = {
    import com.krux.hyperion.common.DurationConverters._
    delay(schedule, by.asJodaPeriodMultiplied(multiplier))
  }

  def delay(schedule: Schedule, by: Period): Schedule = schedule match {
    case s: RecurringSchedule =>
      s.start match {
        case None =>
          s
        case Some(dt) =>
          s.copy(start = Option(dt.plus(by)))
      }
    case OnDemandSchedule =>
      OnDemandSchedule
  }

}
