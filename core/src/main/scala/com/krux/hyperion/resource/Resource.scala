package com.krux.hyperion.resource

import scala.language.implicitConversions

sealed trait Resource[T] {
  def asWorkerGroup: Option[WorkerGroup]
  def asManagedResource: Option[T]

  def toSeq: Seq[T]
}

sealed class WorkerGroupResource[T](wg: WorkerGroup) extends Resource[T] {
  def asWorkerGroup: Option[WorkerGroup] = Option(wg)
  def asManagedResource: Option[T] = None
  def toSeq: Seq[T] = Seq.empty
}

sealed class ManagedResource[T](resource: T) extends Resource[T] {
  def asWorkerGroup: Option[WorkerGroup] = None
  def asManagedResource: Option[T] = Option(resource)
  def toSeq: Seq[T] = Seq(resource)
}

object Resource {
  def apply[T](wg: WorkerGroup): Resource[T] = new WorkerGroupResource(wg)
  def apply[T](resource: T): Resource[T] = new ManagedResource(resource)

  implicit def workerGroupToWorkerGroupResource[T](workerGroup: WorkerGroup): Resource[T] = Resource(workerGroup)
  implicit def resourceToWorkerGroupResource[T](resource: T): Resource[T] = Resource(resource)
}
