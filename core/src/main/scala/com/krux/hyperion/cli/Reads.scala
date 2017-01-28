package com.krux.hyperion.cli

import com.github.nscala_time.time.Imports._
import com.krux.hyperion.Schedule
import com.krux.hyperion.expression.Duration
import org.joda.time.DateTimeConstants
import scopt.Read._

object Reads {

  private lazy val daysOfWeek = Map(
    "monday" -> DateTimeConstants.MONDAY,
    "tuesday" -> DateTimeConstants.TUESDAY,
    "wednesday" -> DateTimeConstants.WEDNESDAY,
    "thursday" -> DateTimeConstants.THURSDAY,
    "friday" -> DateTimeConstants.FRIDAY,
    "saturday" -> DateTimeConstants.SATURDAY,
    "sunday" -> DateTimeConstants.SUNDAY
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

  implicit val dateTimeRead: scopt.Read[DateTime] = reads { x =>
    val dt = x.toLowerCase match {
      case "now" | "today" => DateTime.now
      case "yesterday" => DateTime.yesterday
      case "tomorrow" => DateTime.tomorrow
      case dow if daysOfWeek.keySet contains dow => DateTime.now.withDayOfWeek(daysOfMonth(dow))
      case dom if daysOfMonth.keySet contains dom => DateTime.now.withDayOfMonth(daysOfMonth(dom))
      case d => DateTime.parse(d)
    }

    dt.withZone(DateTimeZone.UTC)
  }

  implicit val scheduleRead: scopt.Read[Schedule] = reads { x =>
    Schedule.cron.startDateTime(dateTimeRead.reads(x))
  }

}
