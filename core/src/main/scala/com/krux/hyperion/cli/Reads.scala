package com.krux.hyperion.cli

import com.krux.hyperion.Schedule
import com.krux.hyperion.expression.Duration
import java.time.{DayOfWeek, ZonedDateTime, ZoneOffset}
import scopt.Read._

object Reads {

  private lazy val daysOfWeek = Map(
    "monday" -> DayOfWeek.MONDAY,
    "tuesday" -> DayOfWeek.TUESDAY,
    "wednesday" -> DayOfWeek.WEDNESDAY,
    "thursday" -> DayOfWeek.THURSDAY,
    "friday" -> DayOfWeek.FRIDAY,
    "saturday" -> DayOfWeek.SATURDAY,
    "sunday" -> DayOfWeek.SUNDAY
  )

  private lazy val daysOfMonth = (1 to 31).flatMap { dom =>
    Seq(dom.toString -> dom, dom % 10 match {
      case 1 => s"${dom}st" -> dom
      case 2 => s"${dom}nd" -> dom
      case 3 => s"${dom}rd" -> dom
      case _ => s"${dom}th" -> dom
    })
  }.toMap

  implicit val durationRead: scopt.Read[Duration] = reads { x => Duration(x) }

  implicit val dateTimeRead: scopt.Read[ZonedDateTime] = reads { x =>
    val dt = x.toLowerCase match {
      case "now" | "today" => ZonedDateTime.now
      case "yesterday" => ZonedDateTime.now.minusDays(1)
      case "tomorrow" => ZonedDateTime.now.plusDays(1)
      case dow if daysOfWeek.keySet contains dow => ZonedDateTime.now.`with`(daysOfWeek(dow))
      case dom if daysOfMonth.keySet contains dom => ZonedDateTime.now.withDayOfMonth(daysOfMonth(dom))
      case d => ZonedDateTime.parse(d)
    }

    dt.withZoneSameInstant(ZoneOffset.UTC)
  }

  implicit val scheduleRead: scopt.Read[Schedule] = reads { x =>
    Schedule.cron.startDateTime(dateTimeRead.reads(x))
  }

}
