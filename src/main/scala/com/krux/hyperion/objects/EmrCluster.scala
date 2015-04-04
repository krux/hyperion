package com.krux.hyperion.objects

trait EmrCluster extends ResourceObject {

  def runHiveScript = HiveActivity(this)

  def runHiveCopy = HiveCopyActivity(this)

  def runMapReduce = MapReduceActivity(this)

  def runPigScript = PigActivity(this)

}
