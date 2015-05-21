package com.krux.hyperion.aws

/**
 * References to an existing aws data pipeline object
 *
 * more details:
 * http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-pipeline-expressions.html
 */
case class AdpRef[+T <: AdpDataPipelineAbstractObject] private (objId: String)

object AdpRef {
  def apply[T <: AdpDataPipelineAbstractObject](obj: T) = new AdpRef[T](obj.id)

  /**
   * @note This is type unsafe as the generic type T is not type checked against the referenced object
   */
  def withRefObjId[T <: AdpDataPipelineAbstractObject](id: String) = new AdpRef[T](id)
}
