package com.krux.hyperion.objects

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.{AdpCopyActivity, AdpDataNode, AdpRef, AdpEc2Resource, AdpActivity}

case class CopyActivity(
  id: String,
  input: DataNode,
  output: DataNode,
  runsOn: Ec2Resource,
  dependsOn: Seq[PipelineActivity] = Seq()
)(
    implicit val hc: HyperionContext
  ) extends PipelineActivity {

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def forClient(client: String) = this.copy(id = s"${id}_${client}")

  override def objects: Iterable[PipelineObject] = Seq(runsOn, input, output) ++ dependsOn

  def serialize = AdpCopyActivity(
    id,
    Some(id),
    AdpRef[AdpDataNode](input.id),
    AdpRef[AdpDataNode](output.id),
    AdpRef[AdpEc2Resource](runsOn.id),
    dependsOn match {
      case Seq() => None
      case deps => Some(deps.map(act => AdpRef[AdpActivity](act.id)))
    }
  )
}