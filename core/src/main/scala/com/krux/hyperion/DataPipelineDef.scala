package com.krux.hyperion

import scala.language.implicitConversions

import org.json4s.JsonDSL._
import org.json4s.{ JArray, JValue }

import com.amazonaws.services.datapipeline.model.{ ParameterObject => AwsParameterObject, PipelineObject => AwsPipelineObject }
import com.krux.hyperion.aws.{ AdpJsonSerializer, AdpParameterSerializer, AdpPipelineSerializer }
import com.krux.hyperion.common.{ PipelineObject, S3UriHelper }
import com.krux.hyperion.workflow.{ WorkflowExpressionImplicits, WorkflowExpression }

/**
 * Base trait of all data pipeline definitions. All data pipelines needs to implement this trait
 */
trait DataPipelineDef
  extends AbstractDataPipelineDef
  with S3UriHelper
  with WorkflowExpressionImplicits {

  def workflow: WorkflowExpression

  final def workflows = Some(workflow)

  def objects: Iterable[PipelineObject] = workflow.toPipelineObjects

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
