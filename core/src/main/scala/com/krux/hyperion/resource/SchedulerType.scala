package com.krux.hyperion.resource

trait SchedulerType

case object ParallelFairScheduler extends SchedulerType {
  override def toString: String = "PARALLEL_FAIR_SCHEDULING"
}

case object ParallelCapacityScheduler extends SchedulerType {
  override def toString: String = "PARALLEL_CAPACITY_SCHEDULING"
}

case object DefaultScheduler extends SchedulerType {
  override def toString: String = "DEFAULT_SCHEDULER"
}
