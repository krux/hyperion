package com.krux.hyperion.objects.aws

import org.json4s._
import org.json4s.JsonDSL._
import com.github.nscala_time.time.Imports._

/**
 * Serializes a aws data pipeline object to JSON
 */
object AdpJsonSerializer {

  val datetimeFormat = "yyyy-MM-dd'T'HH:mm:ss"
  val refKey = "ref"
  val idKey = "id"
  val nameKey = "name"

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
      case JObject(List((refKey, JString(id)))) => AdpRef[AdpDataPipelineObject](id)
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
        def jsonAppend(json: JObject, pair: (String, Either[String, AdpRef[AdpDataPipelineObject]])) = {
          pair match {
            case (k, Left(v)) => json ~ (k -> v)
            case (k, Right(v)) => json ~ (k -> (refKey -> v.objId))
          }
        }
        o.fields.foldLeft((idKey -> o.id) ~ (nameKey -> o.name))(jsonAppend)
    }
  }

}
