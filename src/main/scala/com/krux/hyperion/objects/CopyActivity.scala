package com.krux.hyperion.objects

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.{AdpCopyActivity, AdpDataNode, AdpRef, AdpEc2Resource, AdpActivity}

case class CopyActivity(
  id: String,
  runsOn: Ec2Resource,
  input: Option[DataNode] = None,
  output: Option[DataNode] = None,
  dependsOn: Seq[PipelineActivity] = Seq()
)(
    implicit val hc: HyperionContext
  ) extends PipelineActivity {

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def forClient(client: String) = this.copy(id = s"${id}_${client}")

  def withInput(in: DataNode) = this.copy(input = Some(in))
  def withOutput(out: DataNode) = this.copy(output = Some(out))

  override def objects: Iterable[PipelineObject] = Seq(runsOn) ++ input ++ output ++ dependsOn

  def serialize = AdpCopyActivity(
    id,
    Some(id),
    input.map(in => AdpRef[AdpDataNode](in.id)).get,
    output.map(out => AdpRef[AdpDataNode](out.id)).get,
    AdpRef[AdpEc2Resource](runsOn.id),
    dependsOn match {
      case Seq() => None
      case deps => Some(deps.map(act => AdpRef[AdpActivity](act.id)))
    }
  )
}