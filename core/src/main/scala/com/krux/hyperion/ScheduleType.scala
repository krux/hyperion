package com.krux.hyperion

sealed trait ScheduleType

case object Cron extends ScheduleType {
  override val toString = "cron"
}

case object TimeSeries extends ScheduleType {
  override val toString = "timeseries"
}
