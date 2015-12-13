package com.krux.hyperion.dataformat

import com.krux.hyperion.aws.{ AdpRef, AdpDataFormat }
import com.krux.hyperion.adt.HString
import com.krux.hyperion.common.{ PipelineObject, NamedPipelineObject }

/**
 * The base trait of all data formats
 */
trait DataFormat extends NamedPipelineObject {

  type Self <: DataFormat

  def dataFormatFields: DataFormatFields
  def updateDataFormatFields(fields: DataFormatFields): Self

  def columns = dataFormatFields.columns
  def withColumns(cols: HString*) = updateDataFormatFields(
    dataFormatFields.copy(columns = dataFormatFields.columns ++ cols)
  )

  def serialize: AdpDataFormat

  def ref: AdpRef[AdpDataFormat] = AdpRef(serialize)

  def objects: Iterable[PipelineObject] = None

}
