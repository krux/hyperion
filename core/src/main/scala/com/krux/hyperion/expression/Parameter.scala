package com.krux.hyperion.expression

import scala.language.implicitConversions

import org.joda.time.DateTime

import com.krux.hyperion.adt._
import com.krux.hyperion.aws.AdpParameter
import com.krux.hyperion.common.{HdfsUri, S3Uri}

/**
 * Defines and builds Parameter and returns the specific type instead of the paraent type.
 */
trait ParameterBuilder[T, +Self <: Parameter[T] with ParameterBuilder[T, Self]] { self: Self =>

  def updateParameterFields(fields: ParameterFields): Self

  def withValue(newValue: T): Self = {
    parameterFields.pv.setValue(self, newValue)
    self
  }

  /**
   * This needs to be implemented as a function (instead of a method) as we need to store it with
   * the class at the time the implicit parseString for the type is availble (when the parameter
   * instance is created). Since the calling of this function is mainly used when type is not
   * available such as in {{{List[Parameter[_]]}}}.
   */
  def parseString: (String) => T

  def withValueFromString(stringValue: String) = withValue(parseString(stringValue))

  def withDescription(desc: String): Parameter[T] = updateParameterFields {
    implicit val pv = parameterFields.pv
    parameterFields.copy(description = Option(desc))
  }

}

/**
 * The Parameter class which hides the ParameterBuilder class. Note that the parameter abstract
 * class also belongs to the GenericParameter type class
 */
sealed abstract class Parameter[T : GenericParameter] extends ParameterBuilder[T, Parameter[T]] {

  val env = implicitly[GenericParameter[T]]

  def parameterFields: ParameterFields

  def isEncrypted: Boolean

  def id = parameterFields.id

  def description = parameterFields.description

  def name = if (isEncrypted) s"*my_$id" else s"my_$id"

  def value: Option[T] = parameterFields.pv.getValue(this)

  def isEmpty: Boolean = value.isEmpty

  def evaluate(): T = value.get

  def ref: env.Exp = env.ref(this)

  def `type`: ParameterType.Value = env.`type`

  lazy val serialize: Option[AdpParameter] = Option(
    AdpParameter(
      id = name,
      `type` = `type`.toString,
      description = description,
      optional = HBoolean.False.serialize,
      allowedValues = None,
      isArray = HBoolean.False.serialize,
      `default` = value.map(_.toString)
    )
  )

  override def toString: String = this.ref.serialize

}

object Parameter {

  def apply[T : GenericParameter](id: String)(implicit pv: ParameterValues): UnencryptedParameter[T] =
    new UnencryptedParameter(ParameterFields(id = id))

  def apply[T : GenericParameter](id: String, value: T)(implicit pv: ParameterValues): UnencryptedParameter[T] =
    (new UnencryptedParameter[T](ParameterFields(id = id)) with Evaluatable[T]).withValue(value)

  def unencrypted[T : GenericParameter](id: String)(implicit pv: ParameterValues): UnencryptedParameter[T] =
    apply(id)

  def unencrypted[T : GenericParameter](id: String, value: T)(implicit pv: ParameterValues): UnencryptedParameter[T] =
    apply(id, value)

  def encrypted[T : GenericParameter](id: String)(implicit pv: ParameterValues): EncryptedParameter[T] =
    new EncryptedParameter(ParameterFields(id = id))

  def encrypted[T : GenericParameter](id: String, value: T)(implicit pv: ParameterValues): EncryptedParameter[T] =
    (new EncryptedParameter[T](ParameterFields(id = id)) with Evaluatable[T]).withValue(value)

  implicit def stringParameter2HString(p: Parameter[String]): HString = HString(
    Right(
      new StringExp {
        def content = p.name
      }
    )
  )

  implicit def intParameter2HInt(p: Parameter[Int]): HInt = HInt(
    Right(
      new IntExp {
        def content = p.name
      }
    )
  )

  implicit def doubleParameter2HDouble(p: Parameter[Double]): HDouble = HDouble(
    Right(
      new DoubleExp {
        def content = p.name
      }
    )
  )

  implicit def dateTimeParameter2HDateTime(p: Parameter[DateTime]): HDateTime = HDateTime(
    Right(
      new DateTimeExp {
        def content = p.name
      }
    )
  )

  implicit def durationParameter2HDuration(p: Parameter[Duration]): HDuration = HDuration(
    Right(
      new DurationExp {
        def content = p.name
      }
    )
  )

  implicit def s3UriParameter2HS3Uri(p: Parameter[S3Uri]): HS3Uri = HS3Uri(
    Right(
      new S3UriExp {
        def content = p.name
      }
    )
  )

  implicit def hdfsUriParameter2HHdfsUri(p: Parameter[HdfsUri]): HHdfsUri = HHdfsUri(
    Right(
      new HdfsUriExp {
        def content = p.name
      }
    )
  )

}

/**
 * UnencryptedParameter is subtype of Parameter and belongs to the type class GenericParameter
 */
case class UnencryptedParameter[T : GenericParameter](parameterFields: ParameterFields)
  extends Parameter[T]
  with ParameterBuilder[T, UnencryptedParameter[T]] {

  val parseString = implicitly[GenericParameter[T]].parseString

  def updateParameterFields(fields: ParameterFields) = copy(parameterFields = fields)
  val isEncrypted = false

  def encrypted: EncryptedParameter[T] = new EncryptedParameter[T](parameterFields)
}

/**
 * EncryptedParameter is subtype of Parameter and belongs to the type class GenericParameter
 */
case class EncryptedParameter[T : GenericParameter](parameterFields: ParameterFields)
  extends Parameter[T]
  with ParameterBuilder[T, EncryptedParameter[T]] {

  val parseString = implicitly[GenericParameter[T]].parseString

  def updateParameterFields(fields: ParameterFields) = copy(parameterFields = fields)
  val isEncrypted = true
}
