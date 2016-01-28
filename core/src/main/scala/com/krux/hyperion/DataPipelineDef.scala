package com.krux.hyperion

import scala.language.implicitConversions

import com.amazonaws.services.datapipeline.model.{ ParameterObject => AwsParameterObject, PipelineObject => AwsPipelineObject }
import com.krux.hyperion.activity.MainClass
import com.krux.hyperion.aws.{ AdpJsonSerializer, AdpParameterSerializer, AdpPipelineSerializer }
import com.krux.hyperion.common.{ DefaultObject, PipelineObject, S3UriHelper }
import com.krux.hyperion.expression.{ Parameter, ParameterValues }
import com.krux.hyperion.workflow.WorkflowExpressionImplicits
import org.json4s.JsonDSL._
import org.json4s.{ JArray, JValue }

/**
 * Base trait of all data pipeline definitions. All data pipelines needs to implement this trait
 */
trait DataPipelineDef extends S3UriHelper with WorkflowExpressionImplicits {

  private lazy val context = new HyperionContext()

  implicit def hc: HyperionContext = context

  implicit val pv: ParameterValues = new ParameterValues()

  def schedule: Schedule

  def workflow: WorkflowExpression

  def defaultObject: DefaultObject = DefaultObject(schedule)

  def tags: Map[String, Option[String]] = Map.empty

  def parameters: Iterable[Parameter[_]] = Seq.empty

  def objects: Iterable[PipelineObject] = workflow
    .toPipelineObjects
    .foldLeft(Map[String, PipelineObject]())(flattenPipelineObjects)
    .values

  private def flattenPipelineObjects(r: Map[String, PipelineObject], po: PipelineObject): Map[String, PipelineObject] =
    if (!r.contains(po.id.toString)) {
      r ++ Map(po.id.toString -> po) ++ po.objects.foldLeft(r)(flattenPipelineObjects)
    } else {
      r
    }

  def pipelineName: String = MainClass(this).toString

  def setParameterValue(id: String, value: String, ignoreNonExist: Boolean = true): Unit = {
    val foundParam = parameters.find(_.id == id)
    if (ignoreNonExist) foundParam.foreach(_.withValueFromString(value))
    else foundParam.get.withValueFromString(value)
  }

}

object DataPipelineDef {

  implicit def dataPipelineDef2Json(pd: DataPipelineDef): JValue =
    ("objects" -> JArray(
      AdpJsonSerializer(pd.defaultObject.serialize) ::
      AdpJsonSerializer(pd.schedule.serialize) ::
      pd.objects.map(_.serialize).toList.sortBy(_.id).map(o => AdpJsonSerializer(o)))) ~
    ("parameters" -> JArray(
      pd.parameters.flatMap(_.serialize).map(o => AdpJsonSerializer(o)).toList))

  implicit def dataPipelineDef2Aws(pd: DataPipelineDef): Seq[AwsPipelineObject] =
    AdpPipelineSerializer(pd.defaultObject.serialize) ::
    AdpPipelineSerializer(pd.schedule.serialize) ::
    pd.objects.map(o => AdpPipelineSerializer(o.serialize)).toList

  implicit def dataPipelineDef2AwsParameter(pd: DataPipelineDef): Seq[AwsParameterObject] =
    pd.parameters.flatMap(_.serialize).map(o => AdpParameterSerializer(o)).toList
}
