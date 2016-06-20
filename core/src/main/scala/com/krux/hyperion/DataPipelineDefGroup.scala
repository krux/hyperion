package com.krux.hyperion

import com.amazonaws.services.datapipeline.model.{ ParameterObject => AwsParameterObject,
  PipelineObject => AwsPipelineObject }

import com.krux.hyperion.activity.MainClass
import com.krux.hyperion.aws.{ AdpParameterSerializer, AdpPipelineSerializer }
import com.krux.hyperion.common.{ DefaultObject, S3UriHelper, HdfsUriHelper }
import com.krux.hyperion.expression.{ ParameterValues, Parameter }
import com.krux.hyperion.workflow.{ WorkflowExpression, WorkflowExpressionImplicits }


trait DataPipelineDefGroup
  extends S3UriHelper
  with HdfsUriHelper
  with WorkflowExpressionImplicits {

  def nameKeySeparator = DataPipelineDefGroup.DefaultNameKeySeparator

  private lazy val context = new HyperionContext()

  implicit def hc: HyperionContext = context

  implicit val pv: ParameterValues = new ParameterValues()

  def pipelineName: String = MainClass(this).toString

  def schedule: Schedule

  def defaultObject: DefaultObject = DefaultObject(schedule)

  def parameters: Iterable[Parameter[_]] = Seq.empty

  def tags: Map[String, Option[String]] = Map.empty

  def workflows: Map[WorkflowKey, WorkflowExpression]

  def ungroup(): Map[WorkflowKey, DataPipelineDef] = workflows.map { case (key, workflow) =>
    (
      key,
      DataPipelineDefWrapper(
        hc,
        DataPipelineDefGroup.pipelineNameForKey(this, key),
        schedule,
        () => workflow,
        tags,
        parameters
      )
    )
  }

  /**
   * @param ignoreNonExist ignores the parameter with id unknown to the definition
   */
  def setParameterValue(id: String, value: String, ignoreNonExist: Boolean = true): Unit = {
    val foundParam = parameters.find(_.id == id)
    if (ignoreNonExist) foundParam.foreach(_.withValueFromString(value))
    else foundParam.get.withValueFromString(value)
  }

  def toAwsParameters: Seq[AwsParameterObject] =
    parameters.flatMap(_.serialize).map(o => AdpParameterSerializer(o)).toList

  def toAwsPipelineObjects: Map[WorkflowKey, Seq[AwsPipelineObject]] =
    workflows.mapValues( workflow =>
      AdpPipelineSerializer(defaultObject.serialize) ::
      AdpPipelineSerializer(schedule.serialize) ::
      workflow.toPipelineObjects.map(o => AdpPipelineSerializer(o.serialize)).toList
    )

}

object DataPipelineDefGroup {

  final val DefaultNameKeySeparator = "#"

  final def pipelineNameForKey(pipelineDef: DataPipelineDefGroup, key: WorkflowKey) =
    pipelineDef.pipelineName + key.map(pipelineDef.nameKeySeparator + _).getOrElse("")

}
