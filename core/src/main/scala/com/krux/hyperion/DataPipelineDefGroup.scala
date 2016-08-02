package com.krux.hyperion

import com.amazonaws.services.datapipeline.model.{ParameterObject => AwsParameterObject, PipelineObject => AwsPipelineObject}
import org.json4s.JValue
import org.json4s.JsonAST.JArray
import org.json4s.JsonDSL._

import com.krux.hyperion.activity.MainClass
import com.krux.hyperion.aws.{AdpJsonSerializer, AdpParameterSerializer, AdpPipelineSerializer}
import com.krux.hyperion.common.{DefaultObject, HdfsUriHelper, PipelineObject, S3UriHelper}
import com.krux.hyperion.expression.{Parameter, ParameterValues}
import com.krux.hyperion.workflow.{WorkflowExpression, WorkflowExpressionImplicits}


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

  /**
   * @param ignoreMissing ignores the parameter with id unknown to the definition
   */
  def setParameterValue(id: String, value: String, ignoreMissing: Boolean = true): Unit = {
    val foundParam = parameters.find(_.id == id)
    if (ignoreMissing) foundParam.foreach(_.withValueFromString(value))
    else foundParam.get.withValueFromString(value)
  }

  private[hyperion] def nameForKey(key: WorkflowKey): String =
    pipelineName + key.map(nameKeySeparator + _).getOrElse("")

}

object DataPipelineDefGroup {

  final val DefaultNameKeySeparator = "#"

  implicit class DataPipelineDefGroupOps(dpdg: DataPipelineDefGroup) {
    def ungroup(): Map[WorkflowKey, DataPipelineDef] = dpdg.workflows.map { case (key, workflow) =>
      (
        key,
        DataPipelineDefWrapper(
          dpdg.hc,
          dpdg.nameForKey(key),
          dpdg.schedule,
          () => workflow,
          dpdg.tags,
          dpdg.parameters
        )
      )
    }

    def objects: Map[WorkflowKey, Iterable[PipelineObject]] =
      dpdg.workflows.mapValues(workflow =>
        dpdg.defaultObject +: dpdg.defaultObject.objects ++: workflow.toPipelineObjects.toList
      )

    def toAwsParameters: Seq[AwsParameterObject] =
      dpdg.parameters.flatMap(_.serialize).map(o => AdpParameterSerializer(o)).toList

    def toAwsPipelineObjects: Map[WorkflowKey, Seq[AwsPipelineObject]] =
      objects.mapValues(_.map(_.serialize).toList.sortBy(_.id).map(AdpPipelineSerializer(_)))

    def toJson: JValue =
      ("objects" -> JArray(objects.values.flatten.map(_.serialize).toList.sortBy(_.id).map(AdpJsonSerializer(_)))) ~
      ("parameters" -> JArray(dpdg.parameters.flatMap(_.serialize).map(o => AdpJsonSerializer(o)).toList))

  }
}
