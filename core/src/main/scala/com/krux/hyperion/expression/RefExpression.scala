package com.krux.hyperion.expression

/**
 * Expression that references a run time field
 */
trait RefExpression extends Expression {

  def objectName: String = ""

  def refName: String

  def isRuntime: Boolean

  def content = {
    val prefix = objectName match {
      case "" => ""
      case name => name + "."
    }
    prefix + (if (isRuntime) "@" + refName else refName)
  }


}
