package com.krux.hyperion.parameter

import com.krux.hyperion.aws.AdpParameter

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
