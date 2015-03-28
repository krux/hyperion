package com.krux.hyperion.objects

case class SqlScript(
  sql: String,
  sqlArgument: Seq[String]
)
