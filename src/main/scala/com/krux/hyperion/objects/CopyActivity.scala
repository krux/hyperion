package com.krux.hyperion.objects

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.objects.aws.{AdpCopyActivity, AdpDataNode, AdpRef, AdpEc2Resource,
  AdpActivity, AdpS3DataNode, AdpSqlDataNode}
import com.krux.hyperion.util.PipelineId


/**
 * The activity that copys data from one data node to the other.
 *
 * @note it seems that both input and output format needs to be in CsvDataFormat for this copy to
 * work properly and it needs to be a specific variance of the CSV, for more information check the
 * web page:
 *
 * http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-copyactivity.html
 *
 * From our experience it's really hard to export using TsvDataFormat, in both import and export
 * especially for tasks involving RedshiftCopyActivity. A general rule of thumb is always use
 * default CsvDataFormat for tasks involving both exporting to S3 and copy to redshift.
 */
case class CopyActivity private (
  id: String,
  input: Copyable,
  output: Copyable,
  runsOn: Ec2Resource,
  dependsOn: Seq[PipelineActivity]
)(
    implicit val hc: HyperionContext
) extends PipelineActivity {

  def dependsOn(activities: PipelineActivity*) = this.copy(dependsOn = activities)
  def forClient(client: String) = this.copy(id = s"${id}_${client}")

  override def objects: Iterable[PipelineObject] = Seq(runsOn, input, output) ++ dependsOn

  def serialize = AdpCopyActivity(
    id = id,
    name = Some(id),
    input = input match {
      case n: S3DataNode => AdpRef[AdpS3DataNode](n.id)
      case n: SqlDataNode => AdpRef[AdpSqlDataNode](n.id)
    },
    output = output match {
      case n: S3DataNode => AdpRef[AdpS3DataNode](n.id)
      case n: SqlDataNode => AdpRef[AdpSqlDataNode](n.id)
    },
    runsOn = AdpRef[AdpEc2Resource](runsOn.id),
    dependsOn = dependsOn match {
      case Seq() => None
      case deps => Some(deps.map(act => AdpRef[AdpActivity](act.id)))
    }
  )
}

object CopyActivity {
  def apply(input: Copyable, output: Copyable, runsOn: Ec2Resource)(implicit hc: HyperionContext) =
    new CopyActivity(
      id = PipelineId.generateNewId("CopyActivity"),
      input = input,
      output = output,
      runsOn = runsOn,
      dependsOn = Seq()
    )
}