package com.krux.hyperion.activity

object ActivityHelper {

  def getMainClass(mainClass: Any): Option[String] = mainClass match {
    case mc: String => Option(mc.stripSuffix("$"))
    case mc: Class[_] => getMainClass(mc.getCanonicalName)
    case mc => getMainClass(mc.getClass)
  }

}
