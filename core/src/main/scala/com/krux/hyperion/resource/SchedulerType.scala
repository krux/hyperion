package com.krux.hyperion.resource

trait SchedulerType {
  def serialize: String
  override def toString = serialize
}

case object ParallelFairScheduler extends SchedulerType {
  val serialize = "PARALLEL_FAIR_SCHEDULING"
}

case object ParallelCapacityScheduler extends SchedulerType {
  val serialize = "PARALLEL_CAPACITY_SCHEDULING"
}

case object DefaultScheduler extends SchedulerType {
  val serialize = "DEFAULT_SCHEDULER"
}
