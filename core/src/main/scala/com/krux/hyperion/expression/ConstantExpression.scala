package com.krux.hyperion.expression

import java.time.{ZoneOffset, ZonedDateTime}


trait ConstantExpression[T] extends Expression with Evaluatable[T] {
  def constantValue: T
  def content: String = constantValue.toString

  def evaluate(): T = constantValue
}

case class StringConstantExp(constantValue: String) extends ConstantExpression[String] with StringExp {

  override def content: String = s"""\"$constantValue\""""

}

case class IntConstantExp(constantValue: Int) extends ConstantExpression[Int] with IntExp

case class LongConstantExp(constantValue: Int) extends ConstantExpression[Int] with LongExp

case class DoubleConstantExp(constantValue: Double) extends ConstantExpression[Double] with DoubleExp

case class DateTimeConstantExp(constantValue: ZonedDateTime) extends ConstantExpression[ZonedDateTime] with DateTimeExp {

  override def content: String = {

    val utc = constantValue.withZoneSameLocal(ZoneOffset.UTC)

    val funcDt =
      if (utc.getHour == 0 && utc.getMinute == 0)
        MakeDate(utc.getYear, utc.getMonthValue, utc.getDayOfMonth)
      else
        MakeDateTime(
          utc.getYear,
          utc.getMonthValue,
          utc.getDayOfMonth,
          utc.getHour,
          utc.getMinute)

    funcDt.content

  }

}

case class BooleanConstantExp(constantValue: Boolean) extends ConstantExpression[Boolean] with BooleanExp
