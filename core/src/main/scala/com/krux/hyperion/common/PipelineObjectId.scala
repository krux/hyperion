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

  def withName(name: String, id: PipelineObjectId) = id match {
    case NameGroupObjectId(_, c, r) => NameGroupObjectId(name, c, r)
    case RandomizedObjectId(_, r) => NameGroupObjectId(name, "", r)
    case _ => NameGroupObjectId(name, "")
  }

  def withGroup(group: String, id: PipelineObjectId) = id match {
    case NameGroupObjectId(n, _, r) => NameGroupObjectId(n, group, r)
    case RandomizedObjectId(_, r) => NameGroupObjectId("", group, r)
    case _ => NameGroupObjectId("", group)
  }
}

case class NameGroupObjectId(name: String, group: String, rand: String = UUID.randomUUID.toString) extends PipelineObjectId {

  val uniqueId = (name, group) match {
    case ("", "") => rand
    case ("", g) => (g :: rand :: Nil).mkString("_")
    case (n, "") => (n :: rand :: Nil).mkString("_")
    case (n, g) => (n :: g :: rand :: Nil).mkString("_")
  }

  override def toString = uniqueId
}

case class RandomizedObjectId(seed: String, rand: String = UUID.randomUUID.toString) extends PipelineObjectId {

  val uniqueId = (seed :: rand :: Nil).mkString("_")

  override def toString = uniqueId

}

case class FixedObjectId(seed: String) extends PipelineObjectId {
  override def toString = seed
}

object ScheduleObjectId extends PipelineObjectId {
  override def toString = "PipelineSchedule"
}

object TerminateObjectId extends PipelineObjectId {
  override def toString = "TerminateAction"
}

object DefaultObjectId extends PipelineObjectId {
  override def toString = "Default"
}
