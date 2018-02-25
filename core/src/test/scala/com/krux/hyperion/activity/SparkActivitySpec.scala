package com.krux.hyperion.activity

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.adt.HString
import com.krux.hyperion.common.S3Uri
import com.krux.hyperion.resource.SparkCluster
import com.typesafe.config.ConfigFactory
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

@deprecated("Use EmrActivity with SparkStep instead", "5.0.0")
class SparkActivitySpec extends FlatSpec {
  implicit val hc = new HyperionContext(ConfigFactory.load("example"))

  class MainClass

  it should "be backwards compatible" in {
    val cluster = SparkCluster()
    val testStep = LegacySparkStep(S3Uri("s3://something.jar")).withMainClass(MainClass)
    val activity = SparkActivity(cluster).withSteps(testStep)
    activity.steps.length shouldBe 1
    activity.steps.count(_.scriptRunner.toString.contains("s3://elasticmapreduce/libs/script-runner/script-runner.jar")) shouldEqual 1
    activity.steps.count(_.jobRunner.toString.contains("run-spark-step.sh")) shouldEqual 1
  }
}
