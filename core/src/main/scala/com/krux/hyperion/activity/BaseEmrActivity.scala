package com.krux.hyperion.activity

import com.krux.hyperion.resource.BaseEmrCluster

/**
 * The base trait for activities that run on an Amazon EMR cluster
 */
trait BaseEmrActivity[A <: BaseEmrCluster] extends PipelineActivity[A] {

  type Self <: BaseEmrActivity[A]

}
