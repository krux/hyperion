package com.krux.hyperion.common

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.adt.HString
import com.krux.hyperion.resource.{SparkCluster, Resource}

trait SparkCommandRunner {

  def jobRunner(runsOn: Resource[SparkCluster])(implicit hc: HyperionContext): HString =
    if (runsOn.asManagedResource.exists(_.releaseLabel.nonEmpty)) {
      "spark-submit"
    } else {
      s"${hc.scriptUri}run-spark-step.sh"
    }

  def scriptRunner(runsOn: Resource[SparkCluster]): HString =
    if (runsOn.asManagedResource.exists(_.releaseLabel.nonEmpty)) {
      "command-runner.jar"
    } else {
      "s3://elasticmapreduce/libs/script-runner/script-runner.jar"
    }

}
