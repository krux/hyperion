package com.krux.hyperion.objects.dataformat

import com.krux.hyperion.objects.PipelineObject
import com.krux.hyperion.objects.aws.{AdpDataFormat, AdpRef}

/**
 * The base trait of all data formats
 */
trait DataFormat extends PipelineObject {

  def serialize: AdpDataFormat

  def ref: AdpRef[AdpDataFormat] = AdpRef(serialize)

}
