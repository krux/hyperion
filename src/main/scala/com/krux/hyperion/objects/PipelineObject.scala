package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpDataPipelineAbstractObject, AdpJsonSerializer,
  AdpPipelineSerializer, AdpRef}
import com.amazonaws.services.datapipeline.model.{PipelineObject => AwsPipelineObject}
import scala.language.implicitConversions


/**
 * The base trait of krux data pipeline objects.
 */
trait PipelineObject {

  implicit def uniquePipelineId2String(id: PipelineObjectId): String = id.toString
  implicit def seq2Option[A](anySeq: Seq[A]): Option[Seq[A]] = seqToOption(anySeq)(x => x)

  def id: PipelineObjectId
  def objects: Iterable[PipelineObject] = None
  def serialize: AdpDataPipelineAbstractObject
  def ref: AdpRef[AdpDataPipelineAbstractObject]

  def seqToOption[A, B](anySeq: Seq[A])(transform: A => B) = {
    anySeq match {
      case Seq() => None
      case other => Some(anySeq.map(transform))
    }
  }

}
