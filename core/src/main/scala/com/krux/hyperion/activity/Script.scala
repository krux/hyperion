package com.krux.hyperion.activity

import scala.language.implicitConversions

import com.krux.hyperion.common.S3Uri

sealed trait Script {
  def uri: Option[S3Uri]
  def content: Option[String]
}

sealed case class ScriptUri(uri: Option[S3Uri]) extends Script {
  def content: Option[String] = None
}

sealed case class ScriptContent(content: Option[String]) extends Script {
  def uri: Option[S3Uri] = None
}

object Script {
  def apply(uri: S3Uri): Script = ScriptUri(Option(uri))
  def apply(content: String): Script = ScriptContent(Option(content))

  implicit def s3UriToScript(uri: S3Uri): Script = Script(uri)
  implicit def stringToScript(content: String): Script = Script(content)
}
