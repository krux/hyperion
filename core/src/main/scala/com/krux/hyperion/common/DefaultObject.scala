package com.krux.hyperion.common

import com.krux.hyperion.aws.{AdpDataPipelineDefaultObject, AdpDataPipelineObject, AdpRef}
import com.krux.hyperion.{HyperionContext, Schedule}

/**
  * Defines the overall behaviour of a data pipeline.
  */
case class DefaultObject(schedule: Schedule)(implicit val hc: HyperionContext)
  extends PipelineObject {

     val id = DefaultObjectId

     lazy val serialize = new AdpDataPipelineDefaultObject {
       val fields =
         Map[String, Either[String, AdpRef[AdpDataPipelineObject]]](
           "scheduleType" -> Left(schedule.scheduleType.toString),
           "failureAndRerunMode" -> Left(hc.failureRerunMode),
           "pipelineLogUri" -> Left(hc.logUri),
           "role" -> Left(hc.role),
           "resourceRole" -> Left(hc.resourceRole),
           "schedule" -> Right(schedule.ref)
         // TODO - workerGroup
         // TODO - preActivityTaskConfig
         // TODO - postActivityTaskConfig
         )
     }

     def ref: AdpRef[AdpDataPipelineDefaultObject] = AdpRef(serialize)

     def objects = Seq(schedule)

   }
