package com.krux.hyperion.aws

import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, ZonedDateTime}

import org.json4s._
import org.json4s.JsonDSL._

/**
 * Serializes an AWS DataPipeline object to JSON
 */
object AdpJsonSerializer {

  val datetimeFormat = DateTimeFormatter.ofPattern( "yyyy-MM-dd'T'HH:mm:ss")
  val refKey = "ref"
  val idKey = "id"
  val nameKey = "name"

  case object DateTimeSerializer extends CustomSerializer[ZonedDateTime](format => (
    {
      case JString(s) => ZonedDateTime.parse(s, datetimeFormat).withZoneSameLocal(ZoneOffset.UTC)
      case JNull => null
    },
    {
      case d: ZonedDateTime =>
        JString(d.withZoneSameLocal(ZoneOffset.UTC).format(datetimeFormat))
    }
  ))

  case object AdpRefSerializer extends CustomSerializer[AdpRef[AdpDataPipelineObject]](format => (
    {
      case JObject(List((_, JString(id)))) => AdpRef.withRefObjId[AdpDataPipelineObject](id)
    },
    {
      case AdpRef(objId) => JObject(List(refKey -> JString(objId)))
    }
  ))

  def apply[A <: AdpObject](obj: A)(implicit m: Manifest[A]): JValue = {

    implicit val formats = DefaultFormats + FieldSerializer[A]() + DateTimeSerializer + AdpRefSerializer

    obj match {
      case o: AdpParameter => Extraction.decompose(o)
      case o: AdpDataPipelineObject => Extraction.decompose(o)
      case o: AdpDataPipelineDefaultObject =>
        def jsonAppend(json: JObject, pair: (String, Either[String, AdpRef[AdpDataPipelineAbstractObject]])) = {
          pair match {
            case (k, Left(v)) => json ~ (k -> v)
            case (k, Right(v)) => json ~ (k -> (refKey -> v.objId))
          }
        }
        o.fields.foldLeft((idKey -> o.id) ~ (nameKey -> o.name))(jsonAppend)
    }
  }

}
