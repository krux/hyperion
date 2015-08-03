package com.krux.hyperion.parameter

import com.krux.hyperion.aws.AdpParameter
import com.krux.hyperion.common.S3Uri

case class S3KeyParameter private (
  id: String,
  value: S3Uri,
  description: Option[String],
  isEncrypted: Boolean,
  isOptional: Boolean
) extends Parameter[S3Uri] {

  def withDescription(description: String) = this.copy(description = Option(description))
  def required = this.copy(isOptional = false)
  def encrypted = this.copy(isEncrypted = true)

  lazy val serialize = Option(AdpParameter(
    id = name,
    `type` = "AWS::S3::ObjectKey",
    description = description,
    optional = isOptional.toString,
    allowedValues = None,
    isArray = "false",
    `default` = Option(value.toString)
  ))

}

object S3KeyParameter {
  def apply(id: String, value: S3Uri): S3KeyParameter =
    new S3KeyParameter(
      id = id,
      value = value,
      description = None,
      isEncrypted = false,
      isOptional = true
    )
}
