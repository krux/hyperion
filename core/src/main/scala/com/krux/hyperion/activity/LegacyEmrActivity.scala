package com.krux.hyperion.activity

import com.krux.hyperion.resource.BaseEmrCluster

/**
 * The base trait for activities that run on an Amazon EMR cluster
 */
trait LegacyEmrActivity[A <: BaseEmrCluster] extends PipelineActivity[A] {

  type Self <: LegacyEmrActivity[A]

}
