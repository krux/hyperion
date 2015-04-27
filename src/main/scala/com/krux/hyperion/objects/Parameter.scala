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

  lazy val serialize = AdpParameter(
    id = name,
    `type` = "String",
    description = description,
    optional = false,
    allowedValues = allowedValues match {
      case Seq() => None
      case values => Option(values)
    },
    isArray = false,
    `default` = Option(value)
  )

}

case class IntegerParameter(
  id: String,
  value: Int,
  description: Option[String] = None,
  allowedValues: Seq[Int] = Seq(),
  encrypted: Boolean = false
) extends Parameter {

  lazy val serialize = AdpParameter(
    id = name,
    `type` = "Integer",
    description = description,
    optional = false,
    allowedValues = allowedValues match {
      case Seq() => None
      case values => Option(values.map(_.toString))
    },
    isArray = false,
    `default` = Option(value.toString)
  )

}

case class DoubleParameter(
  id: String,
  value: Double,
  description: Option[String] = None,
  allowedValues: Seq[Double] = Seq(),
  encrypted: Boolean = false
) extends Parameter {

  lazy val serialize = AdpParameter(
    id = name,
    `type` = "Double",
    description = description,
    optional = false,
    allowedValues = allowedValues match {
      case Seq() => None
      case values => Option(values.map(_.toString))
    },
    isArray = false,
    `default` = Option(value.toString)
  )

}

case class S3KeyParameter(
  id: String,
  value: String,
  description: Option[String] = None,
  encrypted: Boolean = false
) extends Parameter {

  lazy val serialize = AdpParameter(
    id = name,
    `type` = "AWS::S3::ObjectKey",
    description = description,
    optional = false,
    allowedValues = None,
    isArray = false,
    `default` = Option(value)
  )

}
