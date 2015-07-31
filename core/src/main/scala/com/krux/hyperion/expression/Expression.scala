package com.krux.hyperion.expression

/**
 * Expression. Expressions are delimited by: "#{" and "}" and the contents of the braces are
 * evaluated by AWS Data Pipeline.
 */
trait Expression {

  def content: String

  override def toString = s"#{$content}"

}
