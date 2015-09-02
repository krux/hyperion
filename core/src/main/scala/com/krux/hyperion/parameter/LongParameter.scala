package com.krux.hyperion.parameter

import com.krux.hyperion.aws.AdpParameter

case class LongParameter private (
  id: String,
  value: Long,
  description: Option[String],
  allowedValues: Seq[Long],
  isEncrypted: Boolean,
  isOptional: Boolean
) extends Parameter[Long] {

  def withDescription(description: String) = this.copy(description = Option(description))
  def withAllowedValues(value: Long*) = this.copy(allowedValues = allowedValues ++ value)
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

object LongParameter {
  def apply(id: String, value: Long): LongParameter =
    new LongParameter(
      id = id,
      value = value,
      description = None,
      allowedValues = Seq.empty,
      isEncrypted = false,
      isOptional = true
    )
}
