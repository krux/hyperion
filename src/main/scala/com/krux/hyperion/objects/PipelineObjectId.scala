package com.krux.hyperion.objects

import java.util.UUID

trait PipelineObjectId

object PipelineObjectId {
  def apply(seed: String) = RamdomisedObjectId(seed)
  def apply(name: String, client: String) = NameClientObjectId(name, client)
  def fixed(seed: String) = FixedObjectId(seed)
}

case class NameClientObjectId(name: String, client: String) extends PipelineObjectId {

  val uniqueId = (name, client) match {
    case ("", "") => UUID.randomUUID.toString
    case ("", c) => s"${c}_${UUID.randomUUID.toString}"
    case (n, "") => s"${n}_${UUID.randomUUID.toString}"
    case (n, c) => s"${n}_${c}_${UUID.randomUUID.toString}"
  }

  override def toString = uniqueId
}

case class RamdomisedObjectId(seed: String) extends PipelineObjectId {

  val uniqueId = seed + "_" + UUID.randomUUID.toString

  override def toString = uniqueId

}

case class FixedObjectId(seed: String) extends PipelineObjectId {
  override def toString = seed
}

object ScheduleObjectId extends PipelineObjectId { override def toString = "PipelineSchedule" }

object TerminateObjectId extends PipelineObjectId { override def toString = "TerminateAction" }

object DefaultObjectId extends PipelineObjectId { override def toString = "Default" }
