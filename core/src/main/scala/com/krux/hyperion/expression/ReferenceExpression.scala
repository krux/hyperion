package com.krux.hyperion.expression

/**
 * Expression that references a run time field
 */
trait ReferenceExpression extends Expression {

  def objectName: Option[String] = None

  def referenceName: String

  def isRuntime: Boolean

  def content: String =
    objectName.map(_ + ".").getOrElse("") + (if (isRuntime) "@" + referenceName else referenceName)

}
