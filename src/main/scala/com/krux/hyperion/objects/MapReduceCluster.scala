package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpEmrCluster, AdpJsonSerializer}
import com.krux.hyperion.HyperionContext

/**
 * Launch a MapReduce cluster
 */
case class MapReduceCluster(
  id: String = "MapReduceCluster",
  taskInstanceCount: Int = 0
)(
  implicit val hc: HyperionContext
) extends EmrCluster {

  assert(taskInstanceCount >= 0)

  val amiVersion = hc.emrAmiVersion
  val coreInstanceCount = 2

  def instanceCount = 1 + coreInstanceCount + taskInstanceCount

  val bootstrapAction = hc.emrEnvironmentUri.map(
    env => s"${hc.scriptUri}deploy-hyperion-emr-env.sh,$env").toList

  val instanceType = hc.emrInstanceType

  val terminateAfter = hc.emrTerminateAfter

  def forClient(client: String) = this.copy(id = s"${id}_${client}")

  def withTaskInstanceCount(n: Int) = this.copy(taskInstanceCount = n)

  def runMapReduce(id: String) =
    MapReduceActivity(
      id = id,
      runsOn = this
    )

  def runPigScript(id: String) =
    PigActivity(
      id = id,
      runsOn = this
    )

  def runHiveScript(name: String, hiveScript: Option[String] = None,
      scriptUri: Option[String] = None, scriptVariable: Option[String] = None,
      input: Option[DataNode] = None, output: Option[DataNode] = None) =
    HiveActivity(
      id = name,
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
