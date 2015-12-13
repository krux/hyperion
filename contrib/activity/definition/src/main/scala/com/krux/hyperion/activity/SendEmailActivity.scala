package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{PipelineObject, PipelineObjectId, ObjectFields, S3Uri}
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.expression.{RunnableObject, Parameter}
import com.krux.hyperion.adt.{ HInt, HDuration, HString, HBoolean, HType, HS3Uri }
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{Resource, Ec2Resource}

case class SendEmailActivity private (
  baseFields: ObjectFields,
  activityFields: ActivityFields[Ec2Resource],
  shellCommandActivityFields: ShellCommandActivityFields,
  jarUri: HS3Uri,
  mainClass: Option[MainClass],
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

  def updateBaseFields(fields: ObjectFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)
  def updateShellCommandActivityFields(fields: ShellCommandActivityFields) = copy(shellCommandActivityFields = fields)

  def withHost(host: HString) = this.copy(host = Option(host))
  def withPort(port: HInt) = this.copy(port = Option(port))
  def withUsername(username: HString) = this.copy(username = Option(username))
  def withPassword(password: Parameter[String]) = this.copy(password = Option(password))
  def withFrom(from: HString) = this.copy(from = Option(from))
  def withTo(to: HString) = this.copy(to = this.to :+ to)
  def withCc(cc: HString) = this.copy(cc = this.cc :+ cc)
  def withBcc(bcc: HString) = this.copy(bcc = this.bcc :+ bcc)
  def withSubject(subject: HString) = this.copy(subject = Option(subject))
  def withBody(body: HString) = this.copy(body = Option(body))
  def withStartTls = this.copy(starttls = true)
  def withDebug = this.copy(debug = true)

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

  override def scriptArguments = (jarUri.serialize: HString) +: (mainClass.fullName: HString) +: arguments

}

object SendEmailActivity extends RunnableObject {

  def apply(runsOn: Resource[Ec2Resource])(implicit hc: HyperionContext): SendEmailActivity =
    new SendEmailActivity(
      baseFields = ObjectFields(PipelineObjectId(SendEmailActivity.getClass)),
      activityFields = ActivityFields(runsOn),
      shellCommandActivityFields = ShellCommandActivityFields(S3Uri(s"${hc.scriptUri}activities/run-jar.sh")),
      jarUri = S3Uri(s"${hc.scriptUri}activities/hyperion-email-activity-current-assembly.jar"),
      mainClass = Option(MainClass("com.krux.hyperion.contrib.activity.email.SendEmailActivity")),
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
