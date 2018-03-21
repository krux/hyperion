package com.krux.hyperion.common

import com.krux.hyperion.adt.{HString, HType}
import com.krux.hyperion.aws.{AdpDataPipelineAbstractObject, AdpDataPipelineDefaultObject, AdpRef}
import com.krux.hyperion.{HyperionContext, OnDemandSchedule, Schedule}

/**
 * Fields used by the DefaultObjects.
 *
 * @param schedule The pipeline schedule.
 * @param properties Additional custom properties to attach to the default object.
 */
case class DefaultObjectFields (
  schedule: Schedule,
  properties: Map[String, Either[HType, PipelineObject]]
)

/**
 * Defines the overall behaviour of a data pipeline.
 */
trait DefaultObject extends PipelineObject {

  type Self <: DefaultObject

  final val id = DefaultObjectId

  def defaultObjectFields: DefaultObjectFields
  def updateDefaultObjectFields(fields: DefaultObjectFields): Self

  def withSchedule(schedule: Schedule): Self = updateDefaultObjectFields(defaultObjectFields.copy(schedule = schedule))
  def withProperty(key: String, value: HType): Self = updateDefaultObjectFields(defaultObjectFields.copy(properties = defaultObjectFields.properties.updated(key, Left(value))))
  def withProperty(key: String, value: PipelineObject): Self = updateDefaultObjectFields(defaultObjectFields.copy(properties = defaultObjectFields.properties.updated(key, Right(value))))

  def objects: Iterable[PipelineObject] = defaultObjectFields.schedule match {
    case OnDemandSchedule => None

    case s => Option(s)
  }

  lazy val serialize = new AdpDataPipelineDefaultObject {
    val scheduleProps: Map[String, Either[HType, PipelineObject]] = defaultObjectFields.schedule match {
      case OnDemandSchedule => Map(
        "scheduleType" -> Left(OnDemandSchedule.scheduleType.serialize)
      )

      case s => Map(
        "schedule" -> Right(s),
        "scheduleType" -> Left(s.scheduleType.serialize)
      )
    }

    val fields: Map[String, Either[String, AdpRef[AdpDataPipelineAbstractObject]]] = (defaultObjectFields.properties ++ scheduleProps).mapValues {
      case Right(p) => Right(p.ref)
      case Left(s) => Left(s.serialize)
    }
  }

  def ref: AdpRef[AdpDataPipelineDefaultObject] = AdpRef(serialize)

}

/**
 * The standard default object.
 *
 * @param defaultObjectFields The fields for the default object.
 */
case class StandardDefaultObject private[hyperion] (
  defaultObjectFields: DefaultObjectFields
) extends DefaultObject {

  type Self = StandardDefaultObject

  def updateDefaultObjectFields(fields: DefaultObjectFields): Self = new StandardDefaultObject(fields)
}

object DefaultObject {

  def apply(schedule: Schedule,
    properties: Map[String, Either[HType, PipelineObject]] = Map.empty)(implicit hc: HyperionContext): DefaultObject = {

    val props: Map[String, Either[HType, PipelineObject]] = Map(
      "failureAndRerunMode" -> Left(hc.failureRerunMode: HString),
      "role" -> Left(hc.role: HString),
      "resourceRole" -> Left(hc.resourceRole: HString)
    ) ++ hc.logUri.map(uri => "pipelineLogUri" -> Left(uri: HString)) ++ properties

    StandardDefaultObject(DefaultObjectFields(schedule, props))
  }

}
