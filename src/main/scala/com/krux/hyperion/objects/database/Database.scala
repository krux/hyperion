package com.krux.hyperion.objects.database

import com.krux.hyperion.objects.PipelineObject
import com.krux.hyperion.objects.aws.{AdpDatabase, AdpRef}

/**
 * The base trait of all database objects
 */
trait Database extends PipelineObject {

  def serialize: AdpDatabase

  def ref: AdpRef[AdpDatabase] = AdpRef(serialize)

}
