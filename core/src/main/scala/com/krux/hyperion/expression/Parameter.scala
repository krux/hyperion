package com.krux.hyperion.expression

import scala.reflect.runtime.universe._
import scala.language.implicitConversions

import org.joda.time.DateTime

import com.krux.hyperion.aws.AdpParameter
import com.krux.hyperion.common.S3Uri
import com.krux.hyperion.adt.{HString, HDouble, HInt, HS3Uri, HDuration, HDateTime, HBoolean}

/**
 * @note need to add support for isOptional, allowedValues and isArray
 */
case class Parameter[T : TypeTag] private (
    id: String,
    description: Option[String],
    isEncrypted: Boolean,
    value: Option[T]  // Parameter may have empty (None) value as place holder
  ) {

  final val name = if (isEncrypted) s"*my_$id" else s"my_$id"

  def withValue(newValue: T): Parameter[T] = this.copy(value = Option(newValue))
  def withDescription(desc: String): Parameter[T] = this.copy(description = Option(desc))
  def encrypted: Parameter[T] = this.copy(isEncrypted = true)

  def isEmpty: Boolean = value.isEmpty

  def reference: TypedExpression = typeOf[T] match {
    case t if t <:< typeOf[Int] => new IntTypedExp { def content = name }
    case t if t <:< typeOf[Double] => new DoubleTypedExp { def content = name }
    case t if t <:< typeOf[String] => new StringTypedExp { def content = name }
    case t if t <:< typeOf[Boolean] => new BooleanTypedExp { def content = name }
    case t if t <:< typeOf[DateTime] => new DateTimeTypedExp { def content = name }
    case t if t <:< typeOf[Duration] => new DurationTypedExp { def content = name }
    case t if t <:< typeOf[S3Uri] => new S3UriTypedExp { def content = name }
    case _ => throw new RuntimeException("Unsupported parameter type")
  }

  def `type`: String = typeOf[T] match {
    case t if t <:< typeOf[Int] => "Integer"
    case t if t <:< typeOf[Double] => "Double"
    case t if t <:< typeOf[String] => "String"
    case t if t <:< typeOf[Boolean] => "String"
    case t if t <:< typeOf[DateTime] => "String"
    case t if t <:< typeOf[Duration] => "String"
    case t if t <:< typeOf[S3Uri] => "AWS::S3::ObjectKey"
    case _ => throw new RuntimeException("Unsupported parameter type")
  }

  def serialize: Option[AdpParameter] = Option(
    AdpParameter(
      id = name,
      `type` = `type`,
      description = description,
      optional = HBoolean.False.toAws,
      allowedValues = None,
      isArray = HBoolean.False.toAws,
      `default` = value.map(_.toString)
    )
  )

  override def toString: String = this.reference.toAws

}

object Parameter {

  def apply[T : TypeTag](id: String): Parameter[T] = new Parameter[T](id, None, false, None)

  def apply[T : TypeTag](id: String, value: T) = new Parameter[T](id, None, false, Option(value))

  implicit def stringParameter2HString(p: Parameter[String]): HString = HString(
    Right(new StringTypedExp { def content = p.name })
  )

  implicit def intParameter2HInt(p: Parameter[Int]): HInt = HInt(
    Right(new IntTypedExp { def content = p.name })
  )

  implicit def doubleParameter2HDouble(p: Parameter[Double]): HDouble = HDouble(
    Right(new DoubleTypedExp { def content = p.name })
  )

  implicit def dateTimeParameter2HDateTime(p: Parameter[DateTime]): HDateTime = HDateTime(
    Right(new DateTimeTypedExp { def content = p.name })
  )

  implicit def durationParameter2HDuration(p: Parameter[Duration]): HDuration = HDuration(
    Right(new DurationTypedExp { def content = p.name })
  )

  implicit def s3UriParameter2HS3Uri(p: Parameter[S3Uri]): HS3Uri = HS3Uri(
    Right(new S3UriTypedExp { def content = p.name })
  )
}
