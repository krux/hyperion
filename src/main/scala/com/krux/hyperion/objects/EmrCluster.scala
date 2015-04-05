package com.krux.hyperion.objects

trait EmrCluster extends ResourceObject {

  def runHiveScript = HiveActivity(runsOn = this)

  def runHiveCopy = HiveCopyActivity(runsOn = this)

  def runMapReduce = MapReduceActivity(runsOn = this)

  def runPigScript = PigActivity(runsOn = this)

}
