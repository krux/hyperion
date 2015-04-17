package com.krux.hyperion.objects

import com.krux.hyperion.objects.aws.{AdpDataPipelineDefaultObject, AdpDataPipelineObject, AdpRef}
import com.krux.hyperion.HyperionContext

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
      )
  }

  def ref: AdpRef[AdpDataPipelineDefaultObject] = AdpRef(serialize)

  override def objects = Seq(schedule)

}
