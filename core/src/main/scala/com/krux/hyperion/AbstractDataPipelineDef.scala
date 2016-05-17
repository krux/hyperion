package com.krux.hyperion

import com.amazonaws.services.datapipeline.model.{ ParameterObject => AwsParameterObject,
  PipelineObject => AwsPipelineObject }
import org.json4s.JsonDSL._
import org.json4s.{ JArray, JValue }

import com.krux.hyperion.activity.MainClass
import com.krux.hyperion.aws.{ AdpJsonSerializer, AdpParameterSerializer, AdpPipelineSerializer }
import com.krux.hyperion.common.DefaultObject
import com.krux.hyperion.expression.{ ParameterValues, Parameter }
import com.krux.hyperion.workflow.WorkflowExpression

trait AbstractDataPipelineDef {

  val NameKeySeparator = "#"

  val emptyKey: WorkflowKey = None

  def pipelineName: String = MainClass(this).toString

  def pipelineNames: Set[String] = Set(pipelineName)

  private lazy val context = new HyperionContext()

  implicit def hc: HyperionContext = context

  implicit val pv: ParameterValues = new ParameterValues()

  def schedule: Schedule

  def workflows: Map[WorkflowKey, WorkflowExpression]

  def defaultObject: DefaultObject = DefaultObject(schedule)

  def tags: Map[String, Option[String]] = Map.empty

  def parameters: Iterable[Parameter[_]] = Seq.empty

  /**
   * @param ignoreNonExist ignores the parameter with id unknown to the definition
   */
  def setParameterValue(id: String, value: String, ignoreNonExist: Boolean = true): Unit = {
    val foundParam = parameters.find(_.id == id)
    if (ignoreNonExist) foundParam.foreach(_.withValueFromString(value))
    else foundParam.get.withValueFromString(value)
  }

  def toJsons: Map[WorkflowKey, JValue] = workflows.mapValues { workflow =>
    ("objects" -> JArray(
      AdpJsonSerializer(defaultObject.serialize) ::
      AdpJsonSerializer(schedule.serialize) ::
      workflow.toPipelineObjects.map(_.serialize).toList.sortBy(_.id).map(o => AdpJsonSerializer(o)))) ~
    ("parameters" -> JArray(
      parameters.flatMap(_.serialize).map(o => AdpJsonSerializer(o)).toList))
  }

  def toAwsPipelineObjects: Map[WorkflowKey, Seq[AwsPipelineObject]] = workflows.mapValues { workflow =>
    AdpPipelineSerializer(defaultObject.serialize) ::
    AdpPipelineSerializer(schedule.serialize) ::
    workflow.toPipelineObjects.map(o => AdpPipelineSerializer(o.serialize)).toList
  }

  def toAwsParameters: Seq[AwsParameterObject] =
    parameters.flatMap(_.serialize).map(o => AdpParameterSerializer(o)).toList

}
