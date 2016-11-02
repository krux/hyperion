package com.krux.hyperion.common

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.adt.HString
import com.krux.hyperion.resource.{SparkCluster, Resource}

trait SparkCommandRunner {

  def jobRunner(runsOn: Resource[SparkCluster])(implicit hc: HyperionContext): HString =
    if (runsOn.asManagedResource.exists(_.releaseLabel.nonEmpty)) {
      // Note calling "spark-submit" directly through common-runner.jar is not supported in the
      // current implementation because SparkStep does not accept local jar and the generated
      // arguments are not spark-submit compatible
      s"${hc.scriptUri}run-spark-step-release-label.sh"
    } else {
      s"${hc.scriptUri}run-spark-step.sh"
    }

  def scriptRunner(runsOn: Resource[SparkCluster]): HString =
    // Note we cannot use "command-runner.jar" until SparkStep is reworked to accept local jars and
    // serialization generates the correct arguments
    "s3://elasticmapreduce/libs/script-runner/script-runner.jar"

}
