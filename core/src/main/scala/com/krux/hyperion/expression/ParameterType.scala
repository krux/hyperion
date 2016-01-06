package com.krux.hyperion.expression

/**
 * AWS Data Pipeline parameter supports the following types, custom types in most cases should be
 * of type StringType
 */
object ParameterType extends Enumeration {

  type ParameterType = Value

  val StringType = Value("String")
  val IntegerType = Value("Integer")
  val DoubleType = Value("Double")
  val S3KeyType = Value("AWS::S3::ObjectKey")
}
