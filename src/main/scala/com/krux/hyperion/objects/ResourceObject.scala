package com.krux.hyperion.objects
import com.krux.hyperion.HyperionContext

trait ResourceObject extends PipelineObject {
  implicit val hc: HyperionContext
  val keyPair = hc.keyPair

  def groupedBy(client: String): ResourceObject
  def named(name: String): ResourceObject

}
