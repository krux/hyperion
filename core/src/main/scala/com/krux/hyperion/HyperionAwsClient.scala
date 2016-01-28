package com.krux.hyperion

import scala.annotation.tailrec
import scala.collection.JavaConversions._

import com.amazonaws.auth.{ DefaultAWSCredentialsProviderChain, STSAssumeRoleSessionCredentialsProvider }
import com.amazonaws.regions.{ Region, Regions }
import com.amazonaws.services.datapipeline._
import com.amazonaws.services.datapipeline.model._
import com.krux.hyperion.DataPipelineDef._
import org.slf4j.LoggerFactory

sealed trait HyperionAwsClient {
  protected lazy val log = LoggerFactory.getLogger(getClass)

  def getPipelineId: Option[String]
  def createPipeline(force: Boolean): Option[String]
  def createPipeline(force: Boolean, activate: Boolean): Boolean
  def activatePipeline(): Boolean
  def deletePipeline(): Boolean
}

case class HyperionAwsClientForPipelineId(client: DataPipelineClient, pipelineId: String) extends HyperionAwsClient {

  def getPipelineId: Option[String] = Option(pipelineId)

  def createPipeline(force: Boolean): Option[String] = None

  def createPipeline(force: Boolean, activate: Boolean): Boolean = false

  def deletePipeline(): Boolean = {
    log.info(s"Deleting pipeline $pipelineId")
    client.deletePipeline(new DeletePipelineRequest().withPipelineId(pipelineId))
    true
  }

  def activatePipeline(): Boolean = {
    log.info(s"Activating pipeline $pipelineId")
    client.activatePipeline(new ActivatePipelineRequest().withPipelineId(pipelineId))
    true
  }

}

case class HyperionAwsClientForName(client: DataPipelineClient, pipelineName: String) extends HyperionAwsClient {

  def getPipelineId: Option[String] = {
    @tailrec
    def queryPipelines(
        ids: List[String] = List.empty,
        request: ListPipelinesRequest = new ListPipelinesRequest()
      ): List[String] = {

      val response = client.listPipelines(request)

      val theseIds: List[String] = response.getPipelineIdList
        .collect { case idName if idName.getName == pipelineName => idName.getId }
        .toList

      if (response.getHasMoreResults) {
        queryPipelines(ids ++ theseIds, new ListPipelinesRequest().withMarker(response.getMarker))
      } else {
        ids ++ theseIds
      }
    }

    queryPipelines() match {
      // if using Hyperion for all DataPipeline management, this should never happen
      case _ :: _ :: other => throw new RuntimeException("Duplicated pipeline name")

      case id :: Nil => Option(id)

      case Nil =>
        log.debug(s"Pipeline ${pipelineName} does not exist")
        None
    }
  }

  def createPipeline(force: Boolean): Option[String] = None

  def createPipeline(force: Boolean, activate: Boolean): Boolean = false

  def activatePipeline(): Boolean = getPipelineId.map(HyperionAwsClientForPipelineId(client, _)).exists(_.activatePipeline())

  def deletePipeline(): Boolean = getPipelineId.map(HyperionAwsClientForPipelineId(client, _)).exists(_.deletePipeline())

}

case class HyperionAwsClientForPipelineDef(client: DataPipelineClient, pipelineDef: DataPipelineDef) extends HyperionAwsClient {

  def getPipelineId: Option[String] =
    HyperionAwsClientForName(client, pipelineDef.pipelineName).getPipelineId

  def createPipeline(force: Boolean): Option[String] = {
    log.info(s"Creating pipeline ${pipelineDef.pipelineName}")

    val pipelineObjects: Seq[PipelineObject] = pipelineDef
    val parameterObjects: Seq[ParameterObject] = pipelineDef

    log.info(s"Pipeline definition has ${pipelineObjects.length} objects")

    getPipelineId match {
      case Some(pipelineId) =>
        log.warn("Pipeline already exists")
        if (force) {
          log.info("Delete the existing pipeline")
          HyperionAwsClientForPipelineId(client, pipelineId).deletePipeline()
          Thread.sleep(10000) // wait until the data pipeline is really deleted
          createPipeline(force)
        } else {
          log.error("Use --force to force pipeline creation")
          None
        }

      case None =>
        val pipelineId = client.createPipeline(
          new CreatePipelineRequest()
            .withUniqueId(pipelineDef.pipelineName)
            .withName(pipelineDef.pipelineName)
            .withTags(pipelineDef.tags.map { case (k, v) => new Tag().withKey(k).withValue(v.getOrElse("")) })
        ).getPipelineId

        log.info(s"Pipeline created: $pipelineId")
        log.info("Uploading pipeline definition")

        val putDefinitionResult = client.putPipelineDefinition(
          new PutPipelineDefinitionRequest()
            .withPipelineId(pipelineId)
            .withPipelineObjects(pipelineObjects)
            .withParameterObjects(parameterObjects)
        )

        putDefinitionResult.getValidationErrors.flatMap(_.getErrors).foreach(log.error)
        putDefinitionResult.getValidationWarnings.flatMap(_.getWarnings).foreach(log.warn)

        if (putDefinitionResult.getErrored) {
          log.error("Failed to create pipeline")
          log.error("Deleting the just created pipeline")
          HyperionAwsClientForPipelineId(client, pipelineId).deletePipeline()
          None
        } else if (putDefinitionResult.getValidationErrors.isEmpty
          && putDefinitionResult.getValidationWarnings.isEmpty) {
          log.info("Successfully created pipeline")
          Option(pipelineId)
        } else {
          log.warn("Successful with warnings")
          Option(pipelineId)
        }
    }
  }

  def createPipeline(force: Boolean, activate: Boolean): Boolean = createPipeline(force).exists { pipelineId =>
    if (activate) HyperionAwsClientForPipelineId(client, pipelineId).activatePipeline() else true
  }

  def activatePipeline(): Boolean = getPipelineId.map(HyperionAwsClientForPipelineId(client, _)).exists(_.activatePipeline())

  def deletePipeline(): Boolean = getPipelineId.map(HyperionAwsClientForPipelineId(client, _)).exists(_.deletePipeline())
}

object HyperionAwsClient {

  def getClient(regionId: Option[String] = None, roleArn: Option[String] = None): DataPipelineClient = {
    val region: Region = Region.getRegion(regionId.map(r => Regions.fromName(r)).getOrElse(Regions.US_EAST_1))
    val defaultProvider = new DefaultAWSCredentialsProviderChain()
    val stsProvider = roleArn.map(new STSAssumeRoleSessionCredentialsProvider(defaultProvider, _, "hyperion"))
    new DataPipelineClient(stsProvider.getOrElse(defaultProvider)).withRegion(region)
  }

  def apply(pipelineId: String, regionId: Option[String], roleArn: Option[String]): HyperionAwsClient =
    new HyperionAwsClientForPipelineId(getClient(regionId, roleArn), pipelineId)

  def apply(pipelineDef: DataPipelineDef, regionId: Option[String], roleArn: Option[String]): HyperionAwsClient =
    new HyperionAwsClientForPipelineDef(getClient(regionId, roleArn), pipelineDef)

}
