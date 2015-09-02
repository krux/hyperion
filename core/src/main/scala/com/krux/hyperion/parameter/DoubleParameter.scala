package com.krux.hyperion.parameter

import com.krux.hyperion.aws.AdpParameter

case class DoubleParameter private (
  id: String,
  value: Double,
  description: Option[String],
  allowedValues: Seq[Double],
  isEncrypted: Boolean,
  isOptional: Boolean
) extends Parameter[Double] {

  def withDescription(description: String) = this.copy(description = Option(description))
  def withAllowedValues(value: Double*) = this.copy(allowedValues = allowedValues ++ value)
  def required = this.copy(isOptional = false)
  def encrypted = this.copy(isEncrypted = true)

  lazy val serialize = Option(AdpParameter(
    id = name,
    `type` = "Double",
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

object DoubleParameter {
  def apply(id: String, value: Double): DoubleParameter =
    new DoubleParameter(
      id = id,
      value = value,
      description = None,
      allowedValues = Seq.empty,
      isEncrypted = false,
      isOptional = true
    )
}
