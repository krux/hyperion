package com.krux.hyperion.common

import com.krux.hyperion.adt.{HString, HInt}
import com.krux.hyperion.aws.{AdpRef, AdpHttpProxy}
import com.krux.hyperion.expression.Parameter

case class HttpProxy private (
  baseFields: BaseFields,
  hostname: Option[HString],
  port: Option[HInt],
  username: Option[HString],
  password: Option[Parameter[String]],
  windowsDomain: Option[HString],
  windowsWorkGroup: Option[HString]
) extends NamedPipelineObject {

  type Self = HttpProxy

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)

  def withHostname(hostname: HString) = this.copy(hostname = Option(hostname))
  def withPort(port: HInt) = this.copy(port = Option(port))
  def withUsername(username: HString) = this.copy(username = Option(username))
  def withPassword(password: Parameter[String]) = {
    assert(password.isEncrypted)
    this.copy(password = Option(password))
  }
  def withWindowsDomain(windowsDomain: HString) = this.copy(windowsDomain = Option(windowsDomain))
  def withWindowsWorkGroup(windowsWorkGroup: HString) = this.copy(windowsWorkGroup = Option(windowsWorkGroup))

  lazy val serialize = AdpHttpProxy(
    id = id,
    name = id.toOption,
    hostname = hostname.map(_.serialize),
    port = port.map(_.serialize),
    username = username.map(_.serialize),
    `*password` = password.map(_.ref.serialize),
    windowsDomain = windowsDomain.map(_.serialize),
    windowsWorkGroup = windowsWorkGroup.map(_.serialize)
  )

  def ref: AdpRef[AdpHttpProxy] = AdpRef(serialize)

  def objects = None

}

object HttpProxy {

  def apply(host: HString, port: HInt): HttpProxy = HttpProxy(
    baseFields = BaseFields(PipelineObjectId(HttpProxy.getClass)),
    hostname = Option(host),
    port = Option(port),
    username = None,
    password = None,
    windowsDomain = None,
    windowsWorkGroup = None
  )

}
