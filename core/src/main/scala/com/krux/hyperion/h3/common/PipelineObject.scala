package com.krux.hyperion.h3.common

import shapeless._

import com.krux.hyperion.aws.{AdpDataPipelineAbstractObject, AdpRef}
import com.krux.hyperion.common.PipelineObjectId
import scala.language.implicitConversions

/**
 * The base trait of krux data pipeline objects.
 */
trait PipelineObject extends Ordered[PipelineObject] {

  type Self <: PipelineObject
  def self = this.asInstanceOf[Self]

  implicit def uniquePipelineId2String(id: PipelineObjectId): String = id.toString
  implicit def seq2Option[A](anySeq: Seq[A]): Option[Seq[A]] = seqToOption(anySeq)(x => x)

  def baseFields: ObjectFields
  def baseFieldsLens: Lens[Self, ObjectFields]

  private def idLens: Lens[Self, PipelineObjectId] = baseFieldsLens >> 'id
  def named(name: String) = idLens.modify(self)(_.named(name))
  def groupedBy(group: String) = idLens.modify(self)(_.groupedBy(group))

  def objects: Iterable[PipelineObject]

  def serialize: AdpDataPipelineAbstractObject
  def ref: AdpRef[AdpDataPipelineAbstractObject]

  def seqToOption[A, B](anySeq: Seq[A])(transform: A => B) = {
    anySeq match {
      case Seq() => None
      case other => Option(anySeq.map(transform))
    }
  }

  def compare(that: PipelineObject): Int =  self.baseFields.id.compare(that.baseFields.id)

}
