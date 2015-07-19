package com.krux.hyperion.parameter

import com.krux.hyperion.aws.AdpParameter

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
