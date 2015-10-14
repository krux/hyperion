package com.krux.hyperion.activity

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.common.{PipelineObject, S3Uri, PipelineObjectId}
import com.krux.hyperion.expression.Duration
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.parameter.Parameter
import com.krux.hyperion.precondition.Precondition
import com.krux.hyperion.resource.{Ec2Resource, Resource}

case class SetS3AclActivity private (
    id: PipelineObjectId,
    scriptUri: Option[S3Uri],
    jarUri: String,
    mainClass: String,
    cannedAcls: Seq[String],
    grants: Seq[String],
    recursive: Boolean,
    s3Uri: Parameter[S3Uri],
    dependsOn: Seq[PipelineActivity],
    tags: Seq[String],
    preconditions: Seq[Precondition],
    onFailAlarms: Seq[SnsAlarm],
    onSuccessAlarms: Seq[SnsAlarm],
    onLateActionAlarms: Seq[SnsAlarm],
    attemptTimeout: Option[Parameter[Duration]],
    lateAfterTimeout: Option[Parameter[Duration]],
    maximumRetries: Option[Parameter[Int]],
    retryDelay: Option[Parameter[Duration]],
    failureAndRerunMode: Option[FailureAndRerunMode],
    runsOn: Resource[Ec2Resource]
  ) extends PipelineActivity {

  def named(name: String) = this.copy(id = id.named(name))
  def groupedBy(group: String) = this.copy(id = id.groupedBy(group))

  private[hyperion] def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = dependsOn ++ activities)
  def onFail(alarms: SnsAlarm*) = this.copy(onFailAlarms = onFailAlarms ++ alarms)
  def onLateAction(alarms: SnsAlarm*) = this.copy(onLateActionAlarms = onLateActionAlarms ++ alarms)
  def onSuccess(alarms: SnsAlarm*) = this.copy(onSuccessAlarms = onSuccessAlarms ++ alarms)
  def whenMet(conditions: Precondition*) = this.copy(preconditions = preconditions ++ conditions)

  def withRecursive = this.copy(recursive = true)

  def objects: Iterable[PipelineObject] = runsOn.toSeq ++ dependsOn ++ preconditions ++ onFailAlarms ++ onSuccessAlarms ++ onLateActionAlarms

  private def buildArgs: Seq[String] =
    Seq(
      cannedAcls match {
        case Seq() => Seq.empty
        case args => Seq("--acl", args.mkString(","))
      },
      grants match {
        case Seq() => Seq.empty
        case args => Seq("--grants", args.mkString(","))
      },
      if (recursive) Seq("--recursive") else Seq.empty,
      Seq(s3Uri.toString)
    ).flatten

  lazy val serialize = AdpShellCommandActivity(
    id = id,
    name = id.toOption,
    command = None,
    scriptUri = scriptUri.map(_.ref),
    scriptArgument = Option(Seq(jarUri, mainClass) ++ buildArgs),
    stdout = None,
    stderr = None,
    stage = Option("false"),
    input = None,
    output = None,
    workerGroup = runsOn.asWorkerGroup.map(_.ref),
    runsOn = runsOn.asManagedResource.map(_.ref),
    dependsOn = seqToOption(dependsOn)(_.ref),
    precondition = seqToOption(preconditions)(_.ref),
    onFail = seqToOption(onFailAlarms)(_.ref),
    onSuccess = seqToOption(onSuccessAlarms)(_.ref),
    onLateAction = seqToOption(onLateActionAlarms)(_.ref),
    attemptTimeout = attemptTimeout.map(_.toString),
    lateAfterTimeout = lateAfterTimeout.map(_.toString),
    maximumRetries = maximumRetries.map(_.toString),
    retryDelay = retryDelay.map(_.toString),
    failureAndRerunMode = failureAndRerunMode.map(_.toString)
  )

}

object SetS3AclActivity extends RunnableObject {
  def apply(s3Uri: S3Uri, acls: Seq[String], grants: Seq[String])(runsOn: Resource[Ec2Resource])(implicit hc: HyperionContext): SetS3AclActivity =
    new SetS3AclActivity(
        id = PipelineObjectId(SetS3AclActivity.getClass),
        scriptUri = Option(S3Uri(s"${hc.scriptUri}activities/run-jar.sh")),
        jarUri = s"${hc.scriptUri}activities/hyperion-s3-activity-current-assembly.jar",
        mainClass = "com.krux.hyperion.contrib.activity.s3.SetS3Acl",
        cannedAcls = acls,
        grants = grants,
        recursive = false,
        s3Uri = s3Uri.toString,
        dependsOn = Seq.empty,
        tags = Seq.empty,
        preconditions = Seq.empty,
        onFailAlarms = Seq.empty,
        onSuccessAlarms = Seq.empty,
        onLateActionAlarms = Seq.empty,
        attemptTimeout = None,
        lateAfterTimeout = None,
        maximumRetries = None,
        retryDelay = None,
        failureAndRerunMode = None,
        runsOn = runsOn
      )
}
