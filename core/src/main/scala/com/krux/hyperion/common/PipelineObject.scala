package com.krux.hyperion.common

import com.krux.hyperion.aws.{ AdpDataPipelineAbstractObject, AdpRef }
import scala.language.implicitConversions

/**
 * The base trait of krux data pipeline objects.
 */
trait PipelineObject extends Ordered[PipelineObject] {

  type Self <: PipelineObject

  implicit def uniquePipelineId2String(id: PipelineObjectId): String = id.toString
  implicit def seq2Option[A](anySeq: Seq[A]): Option[Seq[A]] = seqToOption(anySeq)(x => x)

  def id: PipelineObjectId

  def objects: Iterable[PipelineObject]

  def serialize: AdpDataPipelineAbstractObject
  def ref: AdpRef[AdpDataPipelineAbstractObject]

  def seqToOption[A, B](anySeq: Seq[A])(transform: A => B) = {
    anySeq match {
      case Seq() => None
      case other => Option(anySeq.map(transform))
    }
  }

  def compare(that: PipelineObject): Int = id.compare(that.id)

}
