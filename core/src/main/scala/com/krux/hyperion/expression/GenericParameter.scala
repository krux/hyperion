package com.krux.hyperion.expression

import org.joda.time.{ DateTime, DateTimeZone }

import com.krux.hyperion.common.S3Uri

trait GenericParameter[T] {

  type Exp <: TypedExpression

  def parseString: (String) => T

  def ref(param: Parameter[T]): Exp

  def `type`: String

}

object GenericParameter {

  implicit object IntGenericParameter extends GenericParameter[Int] {

    type Exp = IntExp

    val parseString = (stringValue: String) => stringValue.toInt

    def ref(param: Parameter[Int]): Exp = new Exp with Evaluatable[Int] {
      def content = param.name
      def evaluate() = param.evaluate()
    }

    def `type`: String = "Integer"

  }

  implicit object DoubleGenericParameter extends GenericParameter[Double] {

    type Exp = DoubleExp

    val parseString = (stringValue: String) => stringValue.toDouble

    def ref(param: Parameter[Double]): Exp = new Exp with Evaluatable[Double] {
      def content = param.name
      def evaluate() = param.evaluate()
    }

    def `type`: String = "Double"

  }


  implicit object StringGenericParameter extends GenericParameter[String] {

    type Exp = StringExp

    val parseString = (stringValue: String) => stringValue

    def ref(param: Parameter[String]): Exp = new Exp with Evaluatable[String] {
      def content = param.name
      def evaluate() = param.evaluate()
    }

    def `type`: String = "String"

  }

  implicit object BooleanGenericParameter extends GenericParameter[Boolean] {

    type Exp = BooleanExp

    val parseString = (stringValue: String) => stringValue.toBoolean

    def ref(param: Parameter[Boolean]): Exp = new Exp with Evaluatable[Boolean] {
      def content = param.name
      def evaluate() = param.evaluate()
    }

    // this is not typo, the aws type of this Boolean is String
    def `type`: String = "String"

  }

  implicit object DateTimeGenericParameter extends GenericParameter[DateTime] {

    type Exp = DateTimeExp

    val parseString = (stringValue: String) => new DateTime(stringValue, DateTimeZone.UTC)

    def ref(param: Parameter[DateTime]): Exp = new Exp with Evaluatable[DateTime] {
      def content = param.name
      def evaluate() = param.evaluate()
    }

    // this is not typo, the aws type of this DateTime is String
    def `type`: String = "String"

  }

  implicit object DurationGenericParameter extends GenericParameter[Duration] {

    type Exp = DurationExp

    val parseString = (stringValue: String) => Duration(stringValue)

    def ref(param: Parameter[Duration]): Exp = new Exp with Evaluatable[Duration] {
      def content = param.name
      def evaluate() = param.evaluate()
    }

    // this is not typo, the aws type of this Duration is String
    def `type`: String = "String"

  }


  implicit object S3UriGenericParameter extends GenericParameter[S3Uri] {

    type Exp = S3UriExp

    val parseString = (stringValue: String) => S3Uri(stringValue)

    def ref(param: Parameter[S3Uri]): Exp = new Exp with Evaluatable[S3Uri] {
      def content = param.name
      def evaluate() = param.evaluate()
    }

    def `type`: String = "AWS::S3::ObjectKey"

  }

}
