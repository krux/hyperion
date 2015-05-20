package com.krux.hyperion.objects.parameter

import com.krux.hyperion.objects.aws.AdpParameter

trait Parameter {
  def id: String
  def description: Option[String]
  def encrypted: Boolean

  val name = if (encrypted) s"*my$id" else s"my$id"

  def serialize: AdpParameter
  override def toString = s"#{$name}"
}
