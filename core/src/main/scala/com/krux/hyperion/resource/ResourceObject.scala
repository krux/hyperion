package com.krux.hyperion.resource

import com.krux.hyperion.common.{HttpProxy, PipelineObject}

/**
 * The base trait of all resource objects.
 */
trait ResourceObject extends PipelineObject {

  def groupedBy(client: String): ResourceObject

  def named(name: String): ResourceObject

  def httpProxy: Option[HttpProxy]

  def objects: Iterable[PipelineObject] = httpProxy
}
