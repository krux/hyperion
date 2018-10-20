package com.krux.hyperion.resource

trait WorkerGroup {
  val ref: String
}

object WorkerGroup {

  def apply(workerGroupRef: String): WorkerGroup = new WorkerGroup {
    override val ref: String = workerGroupRef
  }

}
