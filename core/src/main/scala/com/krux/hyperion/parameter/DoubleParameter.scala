package com.krux.hyperion.parameter

import com.krux.hyperion.aws.AdpParameter

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
