package com.krux.hyperion

object ScheduleType extends Enumeration {
  type ScheduleType = Value
  val Cron = Value("cron")
  val TimeSeries = Value("timeseries")
}
