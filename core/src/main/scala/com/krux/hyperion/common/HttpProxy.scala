package com.krux.hyperion.common

import com.krux.hyperion.parameter.Parameter
import com.krux.hyperion.aws.{AdpRef, AdpHttpProxy}

case class HttpProxy private (
  id: PipelineObjectId,
  hostname: Option[String],
  port: Option[Parameter[Int]],
  username: Option[String],
  password: Option[Parameter[String]],
  windowsDomain: Option[String],
  windowsWorkGroup: Option[String]
) extends PipelineObject {

  def withHostname(hostname: String) = this.copy(hostname = Option(hostname))
  def withPort(port: Parameter[Int]) = this.copy(port = Option(port))
  def withUsername(username: String) = this.copy(username = Option(username))
  def withPassword(password: Parameter[String]) = {
    assert(password.isEncrypted)
    this.copy(password = Option(password))
  }
  def withWindowsDomain(windowsDomain: String) = this.copy(windowsDomain = Option(windowsDomain))
  def withWindowsWorkGroup(windowsWorkGroup: String) = this.copy(windowsWorkGroup = Option(windowsWorkGroup))

  lazy val serialize = AdpHttpProxy(
    id = id,
    name = id.toOption,
    hostname = hostname,
    port = port.map(_.toString),
    username = username,
    `*password` = password.map(_.toString),
    windowsDomain = windowsDomain,
    windowsWorkGroup = windowsWorkGroup
  )

  def ref: AdpRef[AdpHttpProxy] = AdpRef(serialize)

  def objects = None

}

object HttpProxy {
  def apply(host: String, port: Parameter[Int]): HttpProxy = HttpProxy(
    id = PipelineObjectId(HttpProxy.getClass),
    hostname = Option(host),
    port = Option(port),
    username = None,
    password = None,
    windowsDomain = None,
    windowsWorkGroup = None
  )
}
