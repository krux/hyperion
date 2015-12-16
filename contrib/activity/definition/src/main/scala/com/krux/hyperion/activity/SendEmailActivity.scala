package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{PipelineObject, PipelineObjectId, BaseFields, S3Uri}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.{RunnableObject, Parameter}
import com.krux.hyperion.adt.{ HInt, HDuration, HString, HBoolean, HType, HS3Uri }
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{Resource, Ec2Resource}

case class SendEmailActivity private (
  baseFields: BaseFields,
  activityFields: ActivityFields[Ec2Resource],
  shellCommandActivityFields: ShellCommandActivityFields,
  jarUri: HS3Uri,
  mainClass: HString,
  host: Option[HString],
  port: Option[HInt],
  username: Option[HString],
  password: Option[Parameter[String]],
  from: Option[HString],
  to: Seq[HString],
  cc: Seq[HString],
  bcc: Seq[HString],
  subject: Option[HString],
  body: Option[HString],
  starttls: HBoolean,
  debug: HBoolean
) extends BaseShellCommandActivity {

  type Self = SendEmailActivity

  require(password.forall(_.isEncrypted), "The password must be an encrypted string parameter")

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)
  def updateShellCommandActivityFields(fields: ShellCommandActivityFields) = copy(shellCommandActivityFields = fields)

  def withHost(host: HString) = copy(host = Option(host))
  def withPort(port: HInt) = copy(port = Option(port))
  def withUsername(username: HString) = copy(username = Option(username))
  def withPassword(password: Parameter[String]) = copy(password = Option(password))
  def withFrom(from: HString) = copy(from = Option(from))
  def withTo(to: HString) = copy(to = this.to :+ to)
  def withCc(cc: HString) = copy(cc = this.cc :+ cc)
  def withBcc(bcc: HString) = copy(bcc = this.bcc :+ bcc)
  def withSubject(subject: HString) = copy(subject = Option(subject))
  def withBody(body: HString) = copy(body = Option(body))
  def withStartTls = copy(starttls = true)
  def withDebug = copy(debug = true)

  private def arguments: Seq[HType] = Seq(
    host.map(h => Seq[HString]("-H", h)),
    port.map(p => Seq[HType]("-P", p)),
    username.map(u => Seq[HString]("-u", u)),
    password.map(p => Seq[HType]("-p", p)),
    from.map(f => Seq[HString]("--from", f)),
    Option(to.flatMap(t => Seq[HString]("--to", t))),
    Option(cc.flatMap(c => Seq[HString]("--cc", c))),
    Option(bcc.flatMap(b => Seq[HString]("--bcc", b))),
    subject.map(s => Seq[HString]("-s", s)),
    body.map(b => Seq[HString]("-B", b)),
    if (starttls) Option(Seq[HString]("--starttls")) else None,
    if (debug) Option(Seq[HString]("--debug")) else None
  ).flatten.flatten

  override def scriptArguments = (jarUri.serialize: HString) +: mainClass +: arguments

}

object SendEmailActivity extends RunnableObject {

  def apply(runsOn: Resource[Ec2Resource])(implicit hc: HyperionContext): SendEmailActivity =
    new SendEmailActivity(
      baseFields = BaseFields(PipelineObjectId(SendEmailActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      shellCommandActivityFields = ShellCommandActivityFields(S3Uri(s"${hc.scriptUri}activities/run-jar.sh")),
      jarUri = S3Uri(s"${hc.scriptUri}activities/hyperion-email-activity-current-assembly.jar"),
      mainClass = "com.krux.hyperion.contrib.activity.email.SendEmailActivity",
      host = None,
      port = None,
      username = None,
      password = None,
      from = None,
      to = Seq.empty,
      cc = Seq.empty,
      bcc = Seq.empty,
      subject = None,
      body = None,
      starttls = false,
      debug = false
    )

}
