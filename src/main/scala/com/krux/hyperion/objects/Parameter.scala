package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.AdpParameter

trait Parameter {
  def id: String
  def description: Option[String]
  def encrypted: Boolean

  val name = if (encrypted) s"*my$id" else s"my$id"

  def serialize: AdpParameter
  override def toString = s"#{$name}"
}

case class StringParameter(
  id: String,
  value: String,
  description: Option[String] = None,
  allowedValues: Seq[String] = Seq(),
  encrypted: Boolean = false
) extends Parameter {

  def serialize = AdpParameter(
    name,
    "String",
    description,
    false,
    allowedValues match {
      case Seq() => None
      case values => Some(values)
    },
    false,
    Some(value)
  )

}

case class IntegerParameter(
  id: String,
  value: Int,
  description: Option[String] = None,
  allowedValues: Seq[Int] = Seq(),
  encrypted: Boolean = false
) extends Parameter {

  def serialize = AdpParameter(
    name,
    "Integer",
    description,
    false,
    allowedValues match {
      case Seq() => None
      case values => Some(values.map(_.toString))
    },
    false,
    Some(value.toString)
  )

}

case class DoubleParameter(
  id: String,
  value: Double,
  description: Option[String] = None,
  allowedValues: Seq[Double] = Seq(),
  encrypted: Boolean = false
) extends Parameter {

  def serialize = AdpParameter(
    name,
    "Double",
    description,
    false,
    allowedValues match {
      case Seq() => None
      case values => Some(values.map(_.toString))
    },
    false,
    Some(value.toString)
  )

}

case class S3KeyParameter(
  id: String,
  value: String,
  description: Option[String] = None,
  encrypted: Boolean = false
) extends Parameter {

  def serialize = AdpParameter(
    name,
    "AWS::S3::ObjectKey",
    description,
    false,
    None,
    false,
    Some(value)
  )

}
