package com.krux.hyperion.common

/**
 * The base fields of all pipeline objects.
 */
case class BaseFields(
  id: PipelineObjectId,
  // note: use String instead of HString because name field does not accept expressions
  name: Option[String] = None
)
