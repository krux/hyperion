package com.krux.hyperion.parameter

import com.krux.hyperion.aws.AdpParameter

case class StringParameter private (
  id: String,
  value: String,
  description: Option[String],
  allowedValues: Seq[String],
  isEncrypted: Boolean,
  isOptional: Boolean
) extends Parameter[String] {

  def withDescription(description: String) = this.copy(description = Option(description))
  def withAllowedValues(value: String*) = this.copy(allowedValues = allowedValues ++ value)
  def required = this.copy(isOptional = false)
  def encrypted = this.copy(isEncrypted = true)

  lazy val serialize = Option(AdpParameter(
    id = name,
    `type` = "String",
    description = description,
    optional = isOptional.toString,
    allowedValues = allowedValues match {
      case Seq() => None
      case values => Option(values)
    },
    isArray = "false",
    `default` = Option(value)
  ))

}

object StringParameter {
  def apply(id: String, value: String): StringParameter =
    new StringParameter(
      id = id,
      value = value,
      description = None,
      allowedValues = Seq.empty,
      isEncrypted = false,
      isOptional = true
    )
}
