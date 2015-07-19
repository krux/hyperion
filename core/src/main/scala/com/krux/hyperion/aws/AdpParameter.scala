package com.krux.hyperion.aws

/**
 * AdpParameter is a pipeline parameter definition.
 */
case class AdpParameter(
  /*
   * The unique identifier of the parameter.
   * To mask the value while it is typed or displayed, add an asterisk ('*') as a prefix.
   * For example, *myVariable.
   * Notes that this also encrypts the value before it is stored by AWS Data Pipeline.
   */
  id: String,

  /*
   * The parameter type that defines the allowed range of input values and validation rules. The default is String.
   * String, Integer, Double, or AWS::S3::ObjectKey
   */
  `type`: String = "String",

  /* A description of the parameter. */
  description: Option[String] = None,

  /* Indicates whether the parameter is optional or required. */
  optional: Boolean = false,

  /* Enumerates all permitted values for the parameter. */
  allowedValues: Option[Seq[String]] = None,

  /* Indicates whether the parameter is an array. */
  isArray: Boolean = false,

  /* The default value. */
  `default`: Option[String] = None
) extends AdpObject
