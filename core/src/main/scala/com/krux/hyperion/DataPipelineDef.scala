package com.krux.hyperion

import scala.language.implicitConversions

import com.amazonaws.services.datapipeline.model.{ ParameterObject => AwsParameterObject,
  PipelineObject => AwsPipelineObject }
import org.json4s.JsonDSL._
import org.json4s.{ JArray, JValue }

import com.krux.hyperion.aws.{ AdpJsonSerializer, AdpParameterSerializer, AdpPipelineSerializer }
import com.krux.hyperion.workflow.WorkflowExpression


/**
 * Base trait of all data pipeline definitions. All data pipelines needs to implement this trait
 */
trait DataPipelineDef extends DataPipelineDefGroup {

  /**
   * Workflow to be defined
   */
  def workflow: WorkflowExpression

  final def workflows = Map(EmptyKey -> workflow)

}

object DataPipelineDef {

  implicit def dataPipelineDef2Json(pd: DataPipelineDef): JValue =
    ("objects" -> JArray(
      AdpJsonSerializer(pd.defaultObject.serialize) ::
      AdpJsonSerializer(pd.schedule.serialize) ::
      pd.workflow.toPipelineObjects.map(_.serialize).toList.sortBy(_.id).map(o => AdpJsonSerializer(o)))) ~
    ("parameters" -> JArray(
      pd.parameters.flatMap(_.serialize).map(o => AdpJsonSerializer(o)).toList))

  implicit def dataPipelineDef2Aws(pd: DataPipelineDef): Seq[AwsPipelineObject] =
    AdpPipelineSerializer(pd.defaultObject.serialize) ::
    AdpPipelineSerializer(pd.schedule.serialize) ::
    pd.workflow.toPipelineObjects.map(o => AdpPipelineSerializer(o.serialize)).toList

}
