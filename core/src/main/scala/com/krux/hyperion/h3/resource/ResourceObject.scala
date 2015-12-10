package com.krux.hyperion.h3.resource

import shapeless._

import com.krux.hyperion.h3.common.{ PipelineObject, HttpProxy }
import com.krux.hyperion.adt.{ HString, HBoolean, HDuration }

/**
 * The base trait of all resource objects.
 */
trait ResourceObject extends PipelineObject {

  type Self <: ResourceObject

  def resourceFieldsLens: Lens[Self, ResourceFields]

  private val roleLens = resourceFieldsLens >> 'role
  def role = roleLens.get(self)
  def withRole(r: HString): Self = roleLens.set(self)(Option(r))

  private val resourceRoleLens = resourceFieldsLens >> 'resourceRole
  def resourceRole = resourceRoleLens.get(self)
  def withResourceRole(r: HString): Self = resourceRoleLens.set(self)(Option(r))

  private val keyPairLens = resourceFieldsLens >> 'keyPair
  def keyPair = keyPairLens.get(self)
  def withKeyPair(theKeyPair: HString): Self = keyPairLens.set(self)(Option(theKeyPair))

  private val regionLens = resourceFieldsLens >> 'region
  def region = regionLens.get(self)
  def withRegion(r: HString): Self = regionLens.set(self)(Option(r))

  private val availabilityZoneLens = resourceFieldsLens >> 'availabilityZone
  def availabilityZone = availabilityZoneLens.get(self)
  def withAvailabilityZone(az: HString): Self = availabilityZoneLens.set(self)(Option(az))

  private val subnetIdLens = resourceFieldsLens >> 'subnetId
  def subnetId = subnetIdLens.get(self)
  def withSubnetId(subnet: HString): Self = subnetIdLens.set(self)(Option(subnet))

  private val useOnDemandOnLastAttemptLens = resourceFieldsLens >> 'useOnDemandOnLastAttempt
  def useOnDemandOnLastAttempt = useOnDemandOnLastAttemptLens.get(self)
  def withUseOnDemandOnLastAttempt(use: HBoolean): Self = useOnDemandOnLastAttemptLens.set(self)(Option(use))

  private val initTimeoutLens = resourceFieldsLens >> 'initTimeout
  def initTimeout = initTimeoutLens.get(self)
  def withInitTimeout(timeout: HDuration): Self = initTimeoutLens.set(self)(Option(timeout))

  private val terminateAfterLens = resourceFieldsLens >> 'terminateAfter
  def terminateAfter = terminateAfterLens.get(self)
  def terminateAfter(after: HDuration) = terminateAfterLens.set(self)(Option(after))

  private val actionOnResourceFailureLens = resourceFieldsLens >> 'actionOnResourceFailure
  def actionOnResourceFailure = actionOnResourceFailureLens.get(self)
  def withActionOnResourceFailure(action: ActionOnResourceFailure): Self = actionOnResourceFailureLens.set(self)(Option(action))

  private val actionOnTaskFailureLens = resourceFieldsLens >> 'actionOnTaskFailure
  def actionOnTaskFailure = actionOnTaskFailureLens.get(self)
  def withActionOnTaskFailure(action: ActionOnTaskFailure): Self = actionOnTaskFailureLens.set(self)(Option(action))

  private val httpProxyLens = resourceFieldsLens >> 'httpProxy
  def httpProxy = httpProxyLens.get(self)
  def withHttpProxy(proxy: HttpProxy): Self = httpProxyLens.set(self)(Option(proxy))

  def objects: Iterable[PipelineObject] = httpProxy

}
