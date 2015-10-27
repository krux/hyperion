package com.krux.hyperion.resource

trait SchedulerType {
  def serialize: String
}

case object ParallelFairScheduler extends SchedulerType {
  val serialize = "PARALLEL_FAIR_SCHEDULING"
  override val toString = serialize
}

case object ParallelCapacityScheduler extends SchedulerType {
  val serialize = "PARALLEL_CAPACITY_SCHEDULING"
  override val toString = serialize
}

case object DefaultScheduler extends SchedulerType {
  val serialize = "DEFAULT_SCHEDULER"
  override val toString = serialize
}
