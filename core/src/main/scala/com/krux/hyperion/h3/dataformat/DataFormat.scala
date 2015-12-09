package com.krux.hyperion.h3.dataformat

import shapeless._

import com.krux.hyperion.aws.{ AdpRef, AdpDataFormat }
import com.krux.hyperion.adt.HString
import com.krux.hyperion.h3.common.PipelineObject

/**
 * The base trait of all data formats
 */
trait DataFormat extends PipelineObject {

  type Self <: DataFormat

  def dataFormatFieldsLens: Lens[Self, DataFormatFields]

  private val columnsLens = dataFormatFieldsLens >> 'columns
  def columns = columnsLens.get(self)
  def withColumns(cols: HString*) = columnsLens.modify(self)(_ ++ cols)

  def serialize: AdpDataFormat

  def ref: AdpRef[AdpDataFormat] = AdpRef(serialize)

  def objects: Iterable[PipelineObject] = None

}
