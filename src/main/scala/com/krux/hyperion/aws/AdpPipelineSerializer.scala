package com.krux.hyperion.aws

import org.json4s._
import com.amazonaws.services.datapipeline.model.{Field => AwsField,
  PipelineObject => AwsPipelineObject, ParameterObject => AwsParameterObject,
  ParameterAttribute => AwsParameterAttribute}
import scala.collection.JavaConversions._

object AdpPipelineSerializer {

  private def fieldListToPipelineObj(fieldList: List[JField]) = {

    val (idName, otherFields) = fieldList.partition { case (k, v) => k == "id" || k == "name" }

    val id = idName.collectFirst { case ("id", JString(x)) => x }.get
    val name = idName.collectFirst { case ("name", JString(x)) => x }.get

    val fields = otherFields.flatMap {
      case (k, JString(v)) => List(new AwsField().withKey(k).withStringValue(v))
      case (k, JObject(List(("ref", JString(refKey))))) => List(new AwsField().withKey(k).withRefValue(refKey))
      case (k, JArray(vs)) => vs.map {
        case JString(v) => new AwsField().withKey(k).withStringValue(v)
        case JObject(List(("ref", JString(refKey)))) => new AwsField().withKey(k).withRefValue(refKey)
        case other => throw new Exception(s"$other has unexpected type")
      }
      case other => throw new Exception(s"$other has unexpected type")
    }

    new AwsPipelineObject().withId(id).withName(name).withFields(fields)
  }

  def apply[A <: AdpObject](obj: A)(implicit mf: Manifest[A]) = {
    val jsonObj = AdpJsonSerializer(obj)

    jsonObj match {
      case JObject(fieldList) => fieldListToPipelineObj(fieldList)
      case _ => throw new Exception("Unexpected Type")  // this should never happen
    }
  }

}

object AdpParameterSerializer {

  private def fieldListToParameterObj(fieldList: List[JField]) = {
    val (idField, otherFields) = fieldList.partition { case (k, v) => k == "id" }
    val id = idField.collectFirst { case ("id", JString(x)) => x }.get

    val attributes = otherFields.flatMap {
      case (k, JString(v)) => List(new AwsParameterAttribute().withKey(k).withStringValue(v))
      case (k, JBool(v)) => List(new AwsParameterAttribute().withKey(k).withStringValue(v.toString))
      case (k, JInt(v)) => List(new AwsParameterAttribute().withKey(k).withStringValue(v.toString))
      case (k, JArray(v)) => List(new AwsParameterAttribute().withKey(k).withStringValue(v.map {
          case JString(x) => x
          case JDouble(x) => x.toString
          case JInt(x) => x.toString
          case JBool(x) => x.toString
          case other => throw new Exception(s"$other has unexpected type")
        }.mkString(",")))
      case other => throw new Exception(s"$other has unexpected type")
    }

    new AwsParameterObject().withId(id).withAttributes(attributes)
  }

  def apply[A <: AdpObject](obj: A)(implicit mf: Manifest[A]) = {
    val jsonObj = AdpJsonSerializer(obj)

    jsonObj match {
      case JObject(fieldList) => fieldListToParameterObj(fieldList)
      case _ => throw new Exception("Unexpected Type")  // this should never happen
    }
  }
}
