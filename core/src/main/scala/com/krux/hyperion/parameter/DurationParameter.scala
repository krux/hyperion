package com.krux.hyperion.parameter

import com.krux.hyperion.aws.AdpParameter
import com.krux.hyperion.expression.Duration

case class DurationParameter private (
  id: String,
  value: Duration,
  description: Option[String],
  isEncrypted: Boolean,
  isOptional: Boolean
) extends Parameter[Duration] {

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

object DurationParameter {
  def apply(id: String, value: Duration): DurationParameter =
    new DurationParameter(
      id = id,
      value = value,
      description = None,
      isEncrypted = false,
      isOptional = true
    )
}
