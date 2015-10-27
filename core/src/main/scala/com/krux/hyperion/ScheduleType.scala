package com.krux.hyperion

sealed trait ScheduleType {
  def serialize: String
}

case object Cron extends ScheduleType {
  val serialize: String = "cron"
  override val toString = serialize
}

case object TimeSeries extends ScheduleType {
  val serialize: String = "timeseries"
  override val toString = serialize
}
