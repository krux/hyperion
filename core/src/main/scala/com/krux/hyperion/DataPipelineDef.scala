package com.krux.hyperion

import com.krux.hyperion.aws.{AdpParameterSerializer, AdpPipelineSerializer, AdpJsonSerializer}
import com.krux.hyperion.common.{DefaultObject, PipelineObject}
import com.krux.hyperion.parameter.Parameter

import scala.language.implicitConversions

import org.json4s.JsonDSL._
import org.json4s.{JValue, JArray}

import com.amazonaws.services.datapipeline.model.{ParameterObject => AwsParameterObject}
import com.amazonaws.services.datapipeline.model.{PipelineObject => AwsPipelineObject}

/**
 * Base trait of all data pipeline definitions. All data pipelines needs to implement this trait
 */
trait DataPipelineDef extends HyperionCli {

  private lazy val context = new HyperionContext()

  implicit def hc: HyperionContext = context

  def schedule: Schedule

  def workflow: WorkflowExpression

  def defaultObject = DefaultObject(schedule)

  def tags: Map[String, Option[String]] = Map()

  def parameters: Iterable[Parameter] = Seq()

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

  def pipelineName = this.getClass.getName match {
    case objName if objName.endsWith("$") => objName.dropRight(1)
    case className => className
  }
}

object DataPipelineDef {

  implicit def dataPipelineDef2Json(pd: DataPipelineDef): JValue =
    ("objects" -> JArray(
      AdpJsonSerializer(pd.defaultObject.serialize) ::
      AdpJsonSerializer(pd.schedule.serialize) ::
      pd.objects.map(o => AdpJsonSerializer(o.serialize)).toList)) ~
    ("parameters" -> JArray(
      pd.parameters.map(o => AdpJsonSerializer(o.serialize)).toList))

  implicit def dataPipelineDef2Aws(pd: DataPipelineDef): Seq[AwsPipelineObject] =
    AdpPipelineSerializer(pd.defaultObject.serialize) ::
    AdpPipelineSerializer(pd.schedule.serialize) ::
    pd.objects.map(o => AdpPipelineSerializer(o.serialize)).toList

  implicit def dataPipelineDef2AwsParameter(pd: DataPipelineDef): Seq[AwsParameterObject] =
    pd.parameters.map(o => AdpParameterSerializer(o.serialize)).toList
}
