package com.krux.hyperion

sealed trait ScheduleType {
  def serialize: String
  override val toString = serialize
}

case object Cron extends ScheduleType {
  val serialize: String = "cron"
}

case object TimeSeries extends ScheduleType {
  val serialize: String = "timeseries"
}
