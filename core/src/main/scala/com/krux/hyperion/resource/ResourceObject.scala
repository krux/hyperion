package com.krux.hyperion.resource

import com.krux.hyperion.common.{ PipelineObject, HttpProxy, NamedPipelineObject }
import com.krux.hyperion.adt.{ HString, HBoolean, HDuration }

/**
 * The base trait of all resource objects.
 */
trait ResourceObject extends NamedPipelineObject {

  type Self <: ResourceObject

  def resourceFields: ResourceFields
  def updateResourceFields(fields: ResourceFields): Self

  def role = resourceFields.role
  def withRole(r: HString): Self = updateResourceFields(
    resourceFields.copy(role = Option(r))
  )

  def resourceRole = resourceFields.resourceRole
  def withResourceRole(r: HString): Self = updateResourceFields(
    resourceFields.copy(resourceRole = Option(r))
  )

  def keyPair = resourceFields.keyPair
  def withKeyPair(theKeyPair: HString): Self = updateResourceFields(
    resourceFields.copy(keyPair = Option(theKeyPair))
  )

  def region = resourceFields.region
  def withRegion(r: HString): Self = updateResourceFields(
    resourceFields.copy(region = Option(r))
  )

  def availabilityZone = resourceFields.availabilityZone
  def withAvailabilityZone(az: HString): Self = updateResourceFields(
    resourceFields.copy(availabilityZone = Option(az))
  )

  def subnetId = resourceFields.subnetId
  def withSubnetId(subnet: HString): Self = updateResourceFields(
    resourceFields.copy(subnetId = Option(subnet))
  )

  def useOnDemandOnLastAttempt = resourceFields.useOnDemandOnLastAttempt
  def withUseOnDemandOnLastAttempt(use: HBoolean): Self = updateResourceFields(
    resourceFields.copy(useOnDemandOnLastAttempt = Option(use))
  )

  def initTimeout = resourceFields.initTimeout
  def withInitTimeout(timeout: HDuration): Self = updateResourceFields(
    resourceFields.copy(initTimeout = Option(timeout))
  )

  def terminateAfter = resourceFields.terminateAfter
  def terminateAfter(after: HDuration) = updateResourceFields(
    resourceFields.copy(terminateAfter = Option(after))
  )

  def actionOnResourceFailure = resourceFields.actionOnResourceFailure
  def withActionOnResourceFailure(action: ActionOnResourceFailure): Self = updateResourceFields(
    resourceFields.copy(actionOnResourceFailure = Option(action))
  )

  def actionOnTaskFailure = resourceFields.actionOnTaskFailure
  def withActionOnTaskFailure(action: ActionOnTaskFailure): Self = updateResourceFields(
    resourceFields.copy(actionOnTaskFailure = Option(action))
  )

  def httpProxy = resourceFields.httpProxy
  def withHttpProxy(proxy: HttpProxy): Self = updateResourceFields(
    resourceFields.copy(httpProxy = Option(proxy))
  )

  def objects: Iterable[PipelineObject] = httpProxy

}
