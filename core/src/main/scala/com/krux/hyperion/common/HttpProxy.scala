package com.krux.hyperion.common

import com.krux.hyperion.aws.{AdpRef, AdpHttpProxy}

case class HttpProxy private (
  id: PipelineObjectId,
  hostname: Option[String],
  port: Option[String],
  username: Option[String],
  password: Option[String],
  windowsDomain: Option[String],
  windowsWorkGroup: Option[String]
) extends PipelineObject {

  lazy val serialize = AdpHttpProxy(
    id = id,
    name = id.toOption,
    hostname = hostname,
    port = port,
    username = username,
    `*password` = password,
    windowsDomain = windowsDomain,
    windowsWorkGroup = windowsWorkGroup
  )

  def ref: AdpRef[AdpHttpProxy] = AdpRef(serialize)

  def objects = None

}

object HttpProxy {
  def apply() = HttpProxy(
    id = PipelineObjectId(HttpProxy.getClass),
    hostname = None,
    port = None,
    username = None,
    password = None,
    windowsDomain = None,
    windowsWorkGroup = None
  )
}
