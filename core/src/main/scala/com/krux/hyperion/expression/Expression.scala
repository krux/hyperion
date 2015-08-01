package com.krux.hyperion.expression

/**
 * Expression. Expressions are delimited by: "#{" and "}" and the contents of the braces are
 * evaluated by AWS Data Pipeline.
 */
sealed trait Expression {

  def content: String

  override def toString = s"#{$content}"

}

/**
 * For expressions that returns string.
 */
case class StringExp(content: String) extends Expression

/**
 * For expressions that returns DateTimes
 */
case class DateTimeExp(content: String) extends Expression
