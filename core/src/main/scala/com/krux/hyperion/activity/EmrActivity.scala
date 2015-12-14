package com.krux.hyperion.activity

import com.krux.hyperion.resource.EmrCluster

/**
 * The base trait for activities that run on an Amazon EMR cluster
 */
trait EmrActivity[A <: EmrCluster] extends PipelineActivity[A] {

  type Self <: EmrActivity[A]

}
