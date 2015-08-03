package com.krux.hyperion.parameter

import com.krux.hyperion.aws.AdpParameter

case class BooleanParameter private (
  id: String,
  value: Boolean,
  description: Option[String],
  isEncrypted: Boolean,
  isOptional: Boolean
) extends Parameter[Boolean] {

  def withDescription(description: String) = this.copy(description = Option(description))
  def required = this.copy(isOptional = false)
  def encrypted = this.copy(isEncrypted = true)

  lazy val serialize = Option(AdpParameter(
    id = name,
    `type` = "String",
    description = description,
    optional = isOptional.toString,
    allowedValues = None,
    isArray = "false",
    `default` = Option(value.toString)
  ))

}

object BooleanParameter {
  def apply(id: String, value: Boolean): BooleanParameter =
    new BooleanParameter(
      id = id,
      value = value,
      description = None,
      isEncrypted = false,
      isOptional = true
    )
}
