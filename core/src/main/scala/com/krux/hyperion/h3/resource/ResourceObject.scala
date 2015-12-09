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

  private val resourceRoleLens = resourceFieldsLens >> 'resourceRole
  def resourceRole = resourceRoleLens.get(self)
  def withResourceRole(role: HString) = resourceRoleLens.set(self)(Option(role))

  private val keyPairLens = resourceFieldsLens >> 'keyPair
  def keyPair = keyPairLens.get(self)
  def withKeyPair(theKeyPair: HString) = keyPairLens.set(self)(Option(theKeyPair))

  private val regionLens = resourceFieldsLens >> 'region
  def region = regionLens.get(self)
  def withRegion(r: HString) = regionLens.set(self)(Option(r))

  private val availabilityZoneLens = resourceFieldsLens >> 'availabilityZone
  def availabilityZone = availabilityZoneLens.get(self)
  def withAvailabilityZone(az: HString) = availabilityZoneLens.set(self)(Option(az))

  private val subnetIdLens = resourceFieldsLens >> 'subnetId
  def subnetId = subnetIdLens.get(self)
  def withSubnetId(subnet: HString) = subnetIdLens.set(self)(Option(subnet))

  private val useOnDemandOnLastAttemptLens = resourceFieldsLens >> 'useOnDemandOnLastAttempt
  def useOnDemandOnLastAttempt = useOnDemandOnLastAttemptLens.get(self)
  def withUseOnDemandOnLastAttempt(use: HBoolean) = useOnDemandOnLastAttemptLens.set(self)(Option(use))

  private val initTimeoutLens = resourceFieldsLens >> 'initTimeout
  def initTimeout = initTimeoutLens.get(self)
  def withInitTimeout(timeout: HDuration) = initTimeoutLens.set(self)(Option(timeout))

  private val httpProxyLens = resourceFieldsLens >> 'httpProxy
  def httpProxy = httpProxyLens.get(self)
  def withHttpProxy(proxy: HttpProxy) = httpProxyLens.set(self)(Option(proxy))

  def objects: Iterable[PipelineObject] = httpProxy

}
