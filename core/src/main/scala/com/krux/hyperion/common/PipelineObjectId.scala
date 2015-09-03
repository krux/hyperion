package com.krux.hyperion.common

import java.util.UUID

trait PipelineObjectId extends Ordered[PipelineObjectId] {

  def toOption: Option[String] = Option(this.toString)

  def compare(that: PipelineObjectId): Int =  this.toString.compare(that.toString)

  def named(name: String) = PipelineObjectId.withName(name, this)
  def groupedBy(group: String) = PipelineObjectId.withGroup(group, this)
}

object PipelineObjectId {
  def apply[T](klass: Class[T]) = RandomizedObjectId(klass.getSimpleName.stripSuffix("$"))
  def apply(seed: String) = RandomizedObjectId(seed)
  def apply(name: String, group: String) = NameGroupObjectId(name, group)
  def fixed(seed: String) = FixedObjectId(seed)

  def withName(name: String, id: PipelineObjectId) =
    id match {
      case NameGroupObjectId(_, c) => NameGroupObjectId(name, c)
      case _ => NameGroupObjectId(name, "")
    }

  def withGroup(group: String, id: PipelineObjectId) =
    id match {
      case NameGroupObjectId(n, _) => NameGroupObjectId(n, group)
      case _ => NameGroupObjectId("", group)
    }
}

case class NameGroupObjectId(name: String, group: String) extends PipelineObjectId {

  val uniqueId = (name, group) match {
    case ("", "") => UUID.randomUUID.toString
    case ("", g) => s"${g}_${UUID.randomUUID.toString}"
    case (n, "") => s"${n}_${UUID.randomUUID.toString}"
    case (n, g) => s"${n}_${g}_${UUID.randomUUID.toString}"
  }

  override def toString = uniqueId
}

case class RandomizedObjectId(seed: String) extends PipelineObjectId {

  val uniqueId = seed + "_" + UUID.randomUUID.toString

  override def toString = uniqueId

}

case class FixedObjectId(seed: String) extends PipelineObjectId {
  override val toString = seed
}

object ScheduleObjectId extends PipelineObjectId {
  override val toString = "PipelineSchedule"
}

object TerminateObjectId extends PipelineObjectId {
  override val toString = "TerminateAction"
}

object DefaultObjectId extends PipelineObjectId {
  override val toString = "Default"
}
