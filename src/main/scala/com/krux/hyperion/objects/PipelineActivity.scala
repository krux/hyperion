package com.krux.hyperion.objects

trait PipelineActivity extends PipelineObject {

  def dependsOn(activities: PipelineActivity*): PipelineActivity

}
