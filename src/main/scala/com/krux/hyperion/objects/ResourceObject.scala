package com.krux.hyperion.objects
import com.krux.hyperion.HyperionContext

trait ResourceObject extends PipelineObject {
  def groupedBy(client: String): ResourceObject
  def named(name: String): ResourceObject
}
