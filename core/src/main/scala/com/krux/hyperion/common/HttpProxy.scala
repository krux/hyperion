package com.krux.hyperion.common

import com.krux.hyperion.adt.{ HString, HInt }
import com.krux.hyperion.aws.{ AdpRef, AdpHttpProxy }
import com.krux.hyperion.expression.EncryptedParameter

case class HttpProxy private (
  baseFields: BaseFields,
  hostname: Option[HString],
  port: Option[HInt],
  username: Option[HString],
  password: Option[EncryptedParameter[String]],
  windowsDomain: Option[HString],
  windowsWorkGroup: Option[HString]
) extends NamedPipelineObject {

  type Self = HttpProxy

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)

  def withHostname(hostname: HString) = copy(hostname = Option(hostname))
  def withPort(port: HInt) = copy(port = Option(port))
  def withUsername(username: HString) = copy(username = Option(username))
  def withPassword(password: EncryptedParameter[String]) = {
    assert(password.isEncrypted)
    copy(password = Option(password))
  }
  def withWindowsDomain(windowsDomain: HString) = copy(windowsDomain = Option(windowsDomain))
  def withWindowsWorkGroup(windowsWorkGroup: HString) = copy(windowsWorkGroup = Option(windowsWorkGroup))

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
