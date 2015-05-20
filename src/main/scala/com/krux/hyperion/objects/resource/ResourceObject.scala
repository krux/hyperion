package com.krux.hyperion.objects.resource

import com.krux.hyperion.objects.PipelineObject

/**
 * The base trait of all resource objects.
 */
trait ResourceObject extends PipelineObject {

  def groupedBy(client: String): ResourceObject

  def named(name: String): ResourceObject

}
