package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpDataPipelineAbstractObject, AdpJsonSerializer,
  AdpPipelineSerializer}
import com.amazonaws.services.datapipeline.model.{PipelineObject => AwsPipelineObject}
import scala.language.implicitConversions


/**
 * The base trait of krux data pipeline objects.
 */
trait PipelineObject {

  implicit def uniquePipelineId2String(id: PipelineObjectId) = id.toString

  def id: PipelineObjectId
  def objects: Iterable[PipelineObject] = None
  def serialize: AdpDataPipelineAbstractObject

}
