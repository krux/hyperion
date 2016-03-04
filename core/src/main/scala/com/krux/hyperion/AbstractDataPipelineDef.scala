package com.krux.hyperion

import org.json4s.JsonDSL._
import org.json4s.{ JArray, JValue }

import com.krux.hyperion.activity.MainClass
import com.krux.hyperion.aws.{ AdpJsonSerializer, AdpParameterSerializer, AdpPipelineSerializer }
import com.krux.hyperion.common.DefaultObject
import com.krux.hyperion.expression.{ ParameterValues, Parameter }
import com.krux.hyperion.workflow.WorkflowExpression

trait AbstractDataPipelineDef {

  def pipelineName: String = MainClass(this).toString

  private lazy val context = new HyperionContext()

  implicit def hc: HyperionContext = context

  implicit val pv: ParameterValues = new ParameterValues()

  def schedule: Schedule

  def workflows: Iterable[WorkflowExpression]

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

  def toJsons: Iterable[JValue] =
    workflows.map { workflow =>
      ("objects" -> JArray(
        AdpJsonSerializer(defaultObject.serialize) ::
        AdpJsonSerializer(schedule.serialize) ::
        Nil)) ~
      ("parameters" -> JArray(
        parameters.flatMap(_.serialize).map(o => AdpJsonSerializer(o)).toList))
      ???
    }

}
