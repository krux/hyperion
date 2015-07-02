package com.krux.hyperion.parameter

import scala.language.implicitConversions

case class ParameterOf[T](value: Either[T, Parameter]) {
  def toOption: Option[ParameterOf[T]] = Option(this)
  override def toString: String = value.fold(_.toString, _.toString)
}

object ParameterOf {
  def stringValue(value: String): ParameterOf[String] = ParameterOf(Left(value))
  def intValue(value: Int): ParameterOf[Int] = ParameterOf(Left(value))
  def doubleValue(value: Double): ParameterOf[Double] = ParameterOf(Left(value))

  implicit def stringToParameterOf(value: String): ParameterOf[String] = stringValue(value)
  implicit def optionOfTToParameterOf[T](value: Option[T]): Option[ParameterOf[T]] = value.map(v => ParameterOf(Left(v)))
  implicit def seqOfTToParameterOf[T](value: Seq[T]): Seq[ParameterOf[T]] = value.map(v => ParameterOf(Left(v)))
  implicit def optionSeqOfTToParameterOf[T](value: Option[Seq[T]]): Option[Seq[ParameterOf[T]]] = value.map(_.map(v => ParameterOf(Left(v))))

  implicit def stringParameterToParameterOf(value: StringParameter): ParameterOf[String] = ParameterOf(Right(value))
  implicit def intParameterToParameterOf(value: IntegerParameter): ParameterOf[Int] = ParameterOf(Right(value))
  implicit def doubleParameterToParameterOf(value: DoubleParameter): ParameterOf[Double] = ParameterOf(Right(value))

  implicit def parameterOfToString[T](parameter: ParameterOf[T]): String = parameter.toString
  implicit def optionalParameterOfToString[T](parameter: Option[ParameterOf[T]]): Option[String] = parameter.map(_.toString)
  implicit def seqParameterOfToSeqString[T](parameter: Seq[ParameterOf[T]]): Seq[String] = parameter.map(_.toString)
  implicit def optionalSeqParameterOfToOptionalSeqString[T](parameter: Option[Seq[ParameterOf[T]]]): Option[Seq[String]] =
    parameter.map(_.map(_.toString))
}
