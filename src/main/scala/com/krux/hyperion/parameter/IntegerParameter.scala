package com.krux.hyperion.parameter

import com.krux.hyperion.aws.AdpParameter

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
