package com.krux.hyperion.parameter

import com.krux.hyperion.aws.AdpParameter

case class IntegerParameter private (
  id: String,
  value: Int,
  description: Option[String],
  allowedValues: Seq[Int],
  isEncrypted: Boolean,
  isOptional: Boolean
) extends Parameter[Int] {

  def withDescription(description: String) = this.copy(description = Option(description))
  def withAllowedValues(value: Int*) = this.copy(allowedValues = allowedValues ++ value)
  def required = this.copy(isOptional = false)
  def encrypted = this.copy(isEncrypted = true)

  lazy val serialize = Option(AdpParameter(
    id = name,
    `type` = "Integer",
    description = description,
    optional = isOptional.toString,
    allowedValues = allowedValues match {
      case Seq() => None
      case values => Option(values.map(_.toString))
    },
    isArray = "false",
    `default` = Option(value.toString)
  ))

}

object IntegerParameter {
  def apply(id: String, value: Int): IntegerParameter =
    new IntegerParameter(
      id = id,
      value = value,
      description = None,
      allowedValues = Seq(),
      isEncrypted = false,
      isOptional = true
    )
}
