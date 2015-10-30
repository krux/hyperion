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
) extends Evaluatable[T] { self =>

  final val name = if (isEncrypted) s"*my_$id" else s"my_$id"

  def withValue(newValue: T): Parameter[T] = this.copy(value = Option(newValue))
  def withDescription(desc: String): Parameter[T] = this.copy(description = Option(desc))
  def encrypted: Parameter[T] = this.copy(isEncrypted = true)

  def isEmpty: Boolean = value.isEmpty

  def evaluate(): T = value.get

  def ref: TypedExpression = typeOf[T] match {
    case t if t <:< typeOf[Int] => new IntExp with Evaluatable[Int] {
      def content = name
      def evaluate() = self.evaluate().asInstanceOf[Int]
    }
    case t if t <:< typeOf[Double] => new DoubleExp with Evaluatable[Double] {
      def content = name
      def evaluate() = self.evaluate().asInstanceOf[Double]
    }
    case t if t <:< typeOf[String] => new StringExp with Evaluatable[String] {
      def content = name
      def evaluate() = self.evaluate().asInstanceOf[String]
    }
    case t if t <:< typeOf[Boolean] => new BooleanExp with Evaluatable[Boolean] {
      def content = name
      def evaluate() = self.evaluate().asInstanceOf[Boolean]
    }
    case t if t <:< typeOf[DateTime] => new DateTimeExp with Evaluatable[DateTime] {
      def content = name
      def evaluate() = self.evaluate().asInstanceOf[DateTime]
    }
    case t if t <:< typeOf[Duration] => new DurationExp with Evaluatable[Duration] {
      def content = name
      def evaluate() = self.evaluate().asInstanceOf[Duration]
    }
    case t if t <:< typeOf[S3Uri] => new S3UriExp with Evaluatable[HS3Uri] {
      def content = name
      def evaluate() = self.evaluate().asInstanceOf[S3Uri]
    }
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
      optional = HBoolean.False.serialize,
      allowedValues = None,
      isArray = HBoolean.False.serialize,
      `default` = value.map(_.toString)
    )
  )

  override def toString: String = this.ref.serialize

}

object Parameter {

  def apply[T : TypeTag](id: String): Parameter[T] = new Parameter[T](id, None, false, None)

  def apply[T : TypeTag](id: String, value: T) = new Parameter[T](id, None, false, Option(value))

  implicit def stringParameter2HString(p: Parameter[String]): HString = HString(
    Right(new StringExp { def content = p.name })
  )

  implicit def intParameter2HInt(p: Parameter[Int]): HInt = HInt(
    Right(new IntExp { def content = p.name })
  )

  implicit def doubleParameter2HDouble(p: Parameter[Double]): HDouble = HDouble(
    Right(new DoubleExp { def content = p.name })
  )

  implicit def dateTimeParameter2HDateTime(p: Parameter[DateTime]): HDateTime = HDateTime(
    Right(new DateTimeExp { def content = p.name })
  )

  implicit def durationParameter2HDuration(p: Parameter[Duration]): HDuration = HDuration(
    Right(new DurationExp { def content = p.name })
  )

  implicit def s3UriParameter2HS3Uri(p: Parameter[S3Uri]): HS3Uri = HS3Uri(
    Right(new S3UriExp { def content = p.name })
  )
}
