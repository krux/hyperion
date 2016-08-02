package com.krux.hyperion

sealed trait ScheduleType {
  def serialize: String
  override def toString = serialize
}

case object Cron extends ScheduleType {
  val serialize: String = "cron"
}

case object TimeSeries extends ScheduleType {
  val serialize: String = "timeseries"
}

case object OnDemand extends ScheduleType {
  val serialize: String = "ondemand"
}
