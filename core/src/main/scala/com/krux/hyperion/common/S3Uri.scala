package com.krux.hyperion.common

case class S3Uri(ref: String) {
  assert(ref.startsWith("s3:"), "S3Uri must start with s3 protocol.")

  override val toString: String = ref
}
