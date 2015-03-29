package com.krux.hyperion.objects.aws

import org.json4s._
import org.json4s.JsonDSL._
import com.github.nscala_time.time.Imports._

/**
 * Serializes a aws data pipeline object to JSON
 */
object AdpJsonSerializer {

  val datetimeFormat = "yyyy-MM-dd'T'HH:mm:ss"

  case object DateTimeSerializer extends CustomSerializer[DateTime](format => (
    {
      case JString(s) => new DateTime(s, DateTimeZone.UTC)
      case JNull => null
    },
    {
      case d: DateTime =>
        JString(d.toDateTime(DateTimeZone.UTC).toString(datetimeFormat))
    }
  ))

  case object AdpRefSerializer extends CustomSerializer[AdpRef[AdpDataPipelineObject]](format => (
    {
      case JObject(List(("ref", JString(id)))) => AdpRef[AdpDataPipelineObject](id)
    },
    {
      case AdpRef(objId) => JObject(List("ref" -> JString(objId)))
    }
  ))

  def apply[A <: AdpDataPipelineAbstractObject](obj: A)(implicit m: Manifest[A]): JValue = {

    implicit val formats = DefaultFormats + FieldSerializer[A]() + DateTimeSerializer + AdpRefSerializer

    obj match {
      case o: AdpDataPipelineObject => Extraction.decompose(obj)
      case o: AdpDataPipelineDefaultObject =>
        def jsonAppend(json: JObject, pair: (String, Either[String, AdpRef[AdpDataPipelineObject]])) = {
          pair match {
            case (k, Left(v)) => json ~ (k -> v)
            case (k, Right(v)) => json ~ (k -> ("ref" -> v.objId))
          }
        }
        o.fields.foldLeft(("id" -> o.id) ~ ("name" -> o.name))(jsonAppend)
    }
  }

}
