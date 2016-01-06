package com.krux.hyperion.expression

import org.joda.time.{ DateTime, DateTimeZone }

import com.krux.hyperion.common.S3Uri
import com.krux.hyperion.expression.ParameterType._

/**
 * The type class for parameters where all parameter class should belong to.  This trait defines
 * the standard methods that needs to be implemented to allow specific parameterized type for a
 * Parameter. To add new Parameter types such as {{{Parameter[YourOwnType]}}}, simply bring an
 * implicit object that extends {{{GenericParameter[YourOwnType]}}} in scope.
 */
trait GenericParameter[T] {

  /**
   * The expression type of the parameter
   */
  type Exp <: TypedExpression

  /**
   * This needs to be implemented as a function (instead of a method) as we need to store it with
   * the class at the time the implicit parseString for the type is availble (when the parameter
   * instance is created). Since the calling of this function is mainly used when type is not
   * available such as in {{{List[Parameter[_]]}}}.
   */
  def parseString: (String) => T

  /**
   * Returns the reference expression of the parameter.
   */
  def ref(param: Parameter[T]): Exp

  def `type`: ParameterType

}

/**
 * This companion class defines the following supported parameter types.
 */
object GenericParameter {

  implicit object IntGenericParameter extends GenericParameter[Int] {

    type Exp = IntExp

    val parseString = (stringValue: String) => stringValue.toInt

    def ref(param: Parameter[Int]): Exp = new Exp with Evaluatable[Int] {
      def content = param.name
      def evaluate() = param.evaluate()
    }

    def `type` = StringType

  }

  implicit object DoubleGenericParameter extends GenericParameter[Double] {

    type Exp = DoubleExp

    val parseString = (stringValue: String) => stringValue.toDouble

    def ref(param: Parameter[Double]): Exp = new Exp with Evaluatable[Double] {
      def content = param.name
      def evaluate() = param.evaluate()
    }

    def `type` = DoubleType

  }


  implicit object StringGenericParameter extends GenericParameter[String] {

    type Exp = StringExp

    val parseString = (stringValue: String) => stringValue

    def ref(param: Parameter[String]): Exp = new Exp with Evaluatable[String] {
      def content = param.name
      def evaluate() = param.evaluate()
    }

    def `type` = StringType

  }

  implicit object BooleanGenericParameter extends GenericParameter[Boolean] {

    type Exp = BooleanExp

    val parseString = (stringValue: String) => stringValue.toBoolean

    def ref(param: Parameter[Boolean]): Exp = new Exp with Evaluatable[Boolean] {
      def content = param.name
      def evaluate() = param.evaluate()
    }

    def `type` = StringType

  }

  implicit object DateTimeGenericParameter extends GenericParameter[DateTime] {

    type Exp = DateTimeExp

    val parseString = (stringValue: String) => new DateTime(stringValue, DateTimeZone.UTC)

    def ref(param: Parameter[DateTime]): Exp = new Exp with Evaluatable[DateTime] {
      def content = param.name
      def evaluate() = param.evaluate()
    }

    def `type` = StringType

  }

  implicit object DurationGenericParameter extends GenericParameter[Duration] {

    type Exp = DurationExp

    val parseString = (stringValue: String) => Duration(stringValue)

    def ref(param: Parameter[Duration]): Exp = new Exp with Evaluatable[Duration] {
      def content = param.name
      def evaluate() = param.evaluate()
    }

    def `type` = StringType

  }


  implicit object S3UriGenericParameter extends GenericParameter[S3Uri] {

    type Exp = S3UriExp

    val parseString = (stringValue: String) => S3Uri(stringValue)

    def ref(param: Parameter[S3Uri]): Exp = new Exp with Evaluatable[S3Uri] {
      def content = param.name
      def evaluate() = param.evaluate()
    }

    def `type` = S3KeyType

  }

}
