package com.krux.hyperion.parameter

import com.krux.hyperion.aws.AdpParameter

trait Parameter {
  def id: String
  def description: Option[String]
  def isEncrypted: Boolean

  def name = if (isEncrypted) s"*my$id" else s"my$id"

  def serialize: AdpParameter

  override def toString = s"#{$name}"
}
