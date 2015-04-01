package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpEmrCluster, AdpJsonSerializer}
import com.krux.hyperion.HyperionContext

/**
 * Launch a Spark cluster
 */
case class SparkCluster (
  id: String,
  taskInstanceCount: Int,
  coreInstanceCount: Int,
  instanceType: String,
  amiVersion: String,
  sparkVersion: String,
  terminateAfter: String
)(
  implicit val hc: HyperionContext
) extends EmrCluster {

  assert(taskInstanceCount >= 0)

  def instanceCount = 1 + coreInstanceCount + taskInstanceCount

  val bootstrapAction = s"s3://support.elasticmapreduce/spark/install-spark,-v,$sparkVersion,-x" ::
      hc.emrEnvironmentUri.map(env => s"${hc.scriptUri}deploy-hyperion-emr-env.sh,$env").toList

  def forClient(client: String) = this.copy(id = s"${id}_${client}")
  def withTaskInstanceCount(n: Int) = this.copy(taskInstanceCount = n)

  def runSpark(id: String) = SparkActivity(
    id = id,
    runsOn = this
  )

  def runMapReduce(id: String) = MapReduceActivity(
    id = id,
    runsOn = this
  )

  def runPigScript(id: String) = PigActivity(
    id = id,
    runsOn = this
  )

  def runHiveScript(id: String, hiveScript: Option[String] = None,
      scriptUri: Option[String] = None, scriptVariable: Option[String] = None,
      input: Option[DataNode] = None, output: Option[DataNode] = None) =
    HiveActivity(
      id = id,
      runsOn = this,
      hiveScript = hiveScript,
      scriptUri = scriptUri,
      scriptVariable = scriptVariable,
      input = input,
      output = output
    )

  def runHiveCopy(id: String,
      filterSql: Option[String] = None,
      generatedScriptsPath: Option[String] = None,
      input: Option[DataNode] = None,
      output: Option[DataNode] = None) =
    HiveCopyActivity(
      id = id,
      runsOn = this,
      filterSql = filterSql,
      generatedScriptsPath = generatedScriptsPath,
      input = input,
      output = output
    )

  def serialize = AdpEmrCluster(
    id = id,
    name = Some(id),
    bootstrapAction = bootstrapAction,
    amiVersion = Some(amiVersion),
    masterInstanceType = Some(instanceType),
    coreInstanceType = Some(instanceType),
    coreInstanceCount = Some(coreInstanceCount.toString),
    taskInstanceType = Some(instanceType),
    taskInstanceCount = Some(taskInstanceCount.toString),
    terminateAfter = terminateAfter,
    keyPair = keyPair
  )

}

object SparkCluster {

  def apply()(implicit hc: HyperionContext) = {
    new SparkCluster(
      id = "SparkCluster",
      taskInstanceCount = 0,
      coreInstanceCount = 2,
      instanceType = hc.emrInstanceType,
      amiVersion = hc.emrAmiVersion,
      sparkVersion = hc.sparkVersion,
      terminateAfter = hc.emrTerminateAfter
    )
  }

}
