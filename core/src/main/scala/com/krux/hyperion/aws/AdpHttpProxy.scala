package com.krux.hyperion.aws

/**
  * @param hostname The host of the proxy which Task Runner clients use to connect to AWS services.
  * @param port Port of the proxy host which the Task Runner clients use to connect to AWS services.
  * @param username The username for the proxy.
  * @param *password The password for proxy.
  * @param windowsDomain The Windows domain name for an NTLM proxy.
  * @param windowsWorkGroup The Windows workgroup name for an NTLM proxy.
  */
case class AdpHttpProxy(
  id: String,
  name: Option[String],
  hostname: Option[String],
  port: Option[String],
  username: Option[String],
  `*password`: Option[String],
  windowsDomain: Option[String],
  windowsWorkGroup: Option[String]
) extends AdpDataPipelineObject {

  val `type` = "HttpProxy"

}
