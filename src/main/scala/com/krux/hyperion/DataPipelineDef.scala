package com.krux.hyperion

import com.amazonaws.services.datapipeline.model.{PipelineObject => AwsPipelineObject}
import com.krux.hyperion.objects.aws.{AdpJsonSerializer, AdpPipelineSerializer}
import com.krux.hyperion.objects.{PipelineObject, Schedule, DefaultObject}
import com.krux.hyperion.util.PipelineId
import org.json4s.JsonDSL._
import org.json4s.{JValue, JArray}
import scala.collection.mutable.Map
import scala.language.implicitConversions

/**
 * Base trait of all data pipeline definitions. All data pipelines needs to implement this trait
 */
trait DataPipelineDef {

  private lazy val context = new HyperionContext()
  implicit def hc: HyperionContext = context

  def schedule: Schedule

  def workflow: Iterable[PipelineObject]

  def defaultObject = DefaultObject(schedule)

  def objects: Iterable[PipelineObject] = workflow
    .foldLeft(Map[String, PipelineObject]())(flattenPipelineObjects)
    .map(_._2)

  private def flattenPipelineObjects(r: Map[String, PipelineObject], po: PipelineObject): Map[String, PipelineObject] =
    if (!r.contains(po.id)) {
      r ++ Map(po.id -> po) ++ po.objects.foldLeft(r)(flattenPipelineObjects)
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
      AdpJsonSerializer(pd.defaultObject) ::
      AdpJsonSerializer(pd.schedule.serialize) ::
      pd.objects.map(o => AdpJsonSerializer(o.serialize)).toList))

  implicit def dataPipelineDef2Aws(pd: DataPipelineDef): Seq[AwsPipelineObject] =
    AdpPipelineSerializer(pd.defaultObject) ::
    AdpPipelineSerializer(pd.schedule.serialize) ::
    pd.objects.map(o => AdpPipelineSerializer(o.serialize)).toList

}
