package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.adt.{ HInt, HDuration, HS3Uri, HString, HBoolean, HType }
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{ PipelineObject, S3Uri, PipelineObjectId, BaseFields }
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{ Ec2Resource, Resource }

case class SetS3AclActivity private (
  baseFields: BaseFields,
  activityFields: ActivityFields[Ec2Resource],
  shellCommandActivityFields: ShellCommandActivityFields,
  jarUri: HString,
  mainClass: HString,
  cannedAcls: Seq[CannedAccessControlList.Value],
  grants: Seq[HString],
  recursive: HBoolean,
  s3Uri: HS3Uri
) extends BaseShellCommandActivity {

  type Self = SetS3AclActivity

  def updateBaseFields(fields: BaseFields) = copy(baseFields = fields)
  def updateActivityFields(fields: ActivityFields[Ec2Resource]) = copy(activityFields = fields)
  def updateShellCommandActivityFields(fields: ShellCommandActivityFields) = copy(shellCommandActivityFields = fields)

  def withRecursive = copy(recursive = true)

  private def arguments: Seq[HType] =
    Seq(
      cannedAcls match {
        case Seq() => Seq.empty
        case args => Seq[HString]("--acl", args.mkString(","))
      },
      grants match {
        case Seq() => Seq.empty
        case args => Seq[HString]("--grants", args.mkString(","))
      },
      if (recursive) Seq[HString]("--recursive") else Seq.empty,
      Seq(s3Uri)
    ).flatten

  override def scriptArguments = (jarUri.serialize: HString) +: mainClass +: arguments

}

object SetS3AclActivity extends RunnableObject {

  def apply(
      s3Uri: HS3Uri,
      acls: Seq[CannedAccessControlList.Value] = Seq.empty,
      grants: Seq[HString] = Seq.empty
    )(runsOn: Resource[Ec2Resource])(implicit hc: HyperionContext): SetS3AclActivity = {

    new SetS3AclActivity(
        baseFields = BaseFields(PipelineObjectId(SetS3AclActivity.getClass)),
        activityFields = ActivityFields(runsOn),
        shellCommandActivityFields = ShellCommandActivityFields(S3Uri(s"${hc.scriptUri}activities/run-jar.sh")),
        jarUri = s"${hc.scriptUri}activities/hyperion-s3-activity-current-assembly.jar",
        mainClass = "com.krux.hyperion.contrib.activity.s3.SetS3Acl",
        cannedAcls = acls,
        grants = grants,
        recursive = false,
        s3Uri = s3Uri
      )
  }
}
