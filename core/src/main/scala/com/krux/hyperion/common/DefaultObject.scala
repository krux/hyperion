package com.krux.hyperion.common

import com.krux.hyperion.aws.{AdpDataPipelineDefaultObject, AdpDataPipelineObject, AdpRef}
import com.krux.hyperion.Schedule
import com.krux.hyperion.HyperionContext

/**
  * Defines the overall behaviour of a data pipeline.
  */
case class DefaultObject(schedule: Schedule)(implicit val hc: HyperionContext)
  extends PipelineObject {

  val id = DefaultObjectId

  lazy val serialize = new AdpDataPipelineDefaultObject {
    val fields =
      Map[String, Either[String, AdpRef[AdpDataPipelineObject]]](
        "scheduleType" -> Left(schedule.scheduleType.serialize),
        "failureAndRerunMode" -> Left(hc.failureRerunMode),
        "role" -> Left(hc.role),
        "resourceRole" -> Left(hc.resourceRole),
        "schedule" -> Right(schedule.ref)
        // TODO - workerGroup
        // TODO - preActivityTaskConfig
        // TODO - postActivityTaskConfig
      ) ++ hc.logUri.map(value => value -> Left(value))
  }

  def ref: AdpRef[AdpDataPipelineDefaultObject] = AdpRef(serialize)

  def objects = Seq(schedule)

}
