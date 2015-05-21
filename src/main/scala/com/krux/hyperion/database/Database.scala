package com.krux.hyperion.database

import com.krux.hyperion.aws.{AdpDatabase, AdpRef}
import com.krux.hyperion.common.PipelineObject

/**
 * The base trait of all database objects
 */
trait Database extends PipelineObject {

  def serialize: AdpDatabase

  def ref: AdpRef[AdpDatabase] = AdpRef(serialize)

}
