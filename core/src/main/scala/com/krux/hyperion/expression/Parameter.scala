package com.krux.hyperion.expression

import scala.language.implicitConversions
import scala.reflect.runtime.universe._

import org.joda.time.{DateTime, DateTimeZone}

import com.krux.hyperion.adt.{HString, HDouble, HInt, HS3Uri, HDuration, HDateTime, HBoolean}
import com.krux.hyperion.aws.AdpParameter
import com.krux.hyperion.common.S3Uri
import com.krux.hyperion.DataPipelineDef

/**
 * @note need to add support for isOptional, allowedValues and isArray
 */
case class Parameter[T : TypeTag] private (
  id: String,
  description: Option[String],
  isEncrypted: Boolean
)(implicit pv: ParameterValues) extends Evaluatable[T] { self =>

  final val name = if (isEncrypted) s"*my_$id" else s"my_$id"

  def value: Option[T] = pv.getValue(this)

  def withValue(newValue: T): Parameter[T] = {
    pv.setValue(this, newValue)
    this
  }

  def withValueFromString(newValue: String): Parameter[T] = withValue {
    val v = typeOf[T] match {
      case t if t <:< typeOf[Int] => newValue.toInt
      case t if t <:< typeOf[Double] => newValue.toDouble
      case t if t <:< typeOf[String] => newValue
      case t if t <:< typeOf[Boolean] => newValue.toBoolean
      case t if t <:< typeOf[DateTime] => new DateTime(newValue, DateTimeZone.UTC)
      case t if t <:< typeOf[Duration] => Duration(newValue)
      case t if t <:< typeOf[S3Uri] => S3Uri(newValue)
      case _ => throw new RuntimeException("Unsupported parameter type")
    }
    v.asInstanceOf[T]
  }

  def withDescription(desc: String): Parameter[T] = copy(description = Option(desc))
  def encrypted: Parameter[T] = copy(isEncrypted = true)

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

  def apply[T : TypeTag](id: String)(implicit pv: ParameterValues): Parameter[T] = new Parameter[T](id, None, false)

  def apply[T : TypeTag](id: String, value: T)(implicit pv: ParameterValues) = new Parameter[T](id, None, false).withValue(value)

  implicit def stringParameter2HString(p: Parameter[String]): HString = HString(
    Right(
      new StringExp with Evaluatable[String] {
        def content = p.name
        def evaluate() = p.evaluate()
      }
    )
  )

  implicit def intParameter2HInt(p: Parameter[Int]): HInt = HInt(
    Right(
      new IntExp with Evaluatable[Int] {
        def content = p.name
        def evaluate() = p.evaluate()
      }
    )
  )

  implicit def doubleParameter2HDouble(p: Parameter[Double]): HDouble = HDouble(
    Right(
      new DoubleExp with Evaluatable[Double] {
        def content = p.name
        def evaluate() = p.evaluate()
      }
    )
  )

  implicit def dateTimeParameter2HDateTime(p: Parameter[DateTime]): HDateTime = HDateTime(
    Right(
      new DateTimeExp with Evaluatable[DateTime] {
        def content = p.name
        def evaluate() = p.evaluate()
      }
    )
  )

  implicit def durationParameter2HDuration(p: Parameter[Duration]): HDuration = HDuration(
    Right(
      new DurationExp with Evaluatable[Duration] {
        def content = p.name
        def evaluate() = p.evaluate()
      }
    )
  )

  implicit def s3UriParameter2HS3Uri(p: Parameter[S3Uri]): HS3Uri = HS3Uri(
    Right(
      new S3UriExp with Evaluatable[S3Uri] {
        def content = p.name
        def evaluate() = p.evaluate()
      }
    )
  )
}
