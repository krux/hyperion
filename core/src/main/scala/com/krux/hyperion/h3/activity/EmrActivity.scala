package com.krux.hyperion.h3.activity


import com.krux.hyperion.adt.HString
import com.krux.hyperion.h3.resource.EmrCluster

/**
 * The base trait for activities that run on an Amazon EMR cluster
 */
trait EmrActivity[A <: EmrCluster] extends PipelineActivity[A] {

  type Self <: EmrActivity[A]

}
