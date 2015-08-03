package com.krux.hyperion.dataformat

import com.krux.hyperion.aws.{AdpRef, AdpDataFormat}
import com.krux.hyperion.common.PipelineObject

/**
 * The base trait of all data formats
 */
trait DataFormat extends PipelineObject {

  def serialize: AdpDataFormat

  def ref: AdpRef[AdpDataFormat] = AdpRef(serialize)

  def objects: Iterable[PipelineObject] = None

}
