package com.krux.hyperion.resource

trait SchedulerType

case object ParallelFairScheduler {
  override def toString: String = "PARALLEL_FAIR_SCHEDULING"
}

case object ParallelCapacityScheduler {
  override def toString: String = "PARALLEL_CAPACITY_SCHEDULING"
}

case object DefaultScheduler {
  override def toString: String = "DEFAULT_SCHEDULER"
}
