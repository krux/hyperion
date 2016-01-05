package com.krux.hyperion.expression

case class ParameterFields(
  id: String,
  description: Option[String] = None
)(implicit val pv: ParameterValues)
