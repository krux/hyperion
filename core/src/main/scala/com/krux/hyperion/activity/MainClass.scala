package com.krux.hyperion.activity

import scala.language.implicitConversions

class MainClass private (name: String) {
  override def toString: String = name
}

object MainClass {
  implicit def stringToMainClass(s: String): MainClass = MainClass(s)
  implicit def classToMainClass(c: Class[_]): MainClass = MainClass(c)
  implicit def anyToMainClass(a: Any): MainClass = MainClass(a)

  def apply(mainClass: Any): MainClass = mainClass match {
      case s: String => new MainClass(s.stripSuffix("$"))
      case c: Class[_] => apply(c.getCanonicalName)
      case mc => apply(mc.getClass)
  }
}

