package com.krux.hyperion

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.Random

import com.amazonaws.auth.{ DefaultAWSCredentialsProviderChain, STSAssumeRoleSessionCredentialsProvider }
import com.amazonaws.regions.{ Region, Regions }
import com.amazonaws.services.datapipeline._
import com.amazonaws.services.datapipeline.model._
import com.amazonaws.{ AmazonWebServiceRequest, AmazonServiceException }
import org.slf4j.LoggerFactory

import com.krux.hyperion.DataPipelineDef._


sealed trait HyperionAwsClient {

  def maxRetry: Int

  protected lazy val log = LoggerFactory.getLogger(getClass)

  def throttleRetry[A](func: => A, n: Int = 0): A = {
    if (n < maxRetry)
      try {
        func
      } catch {
        // use startsWith becase the doc says the error code is called "Throttling" but sometimes
        // we see "ThrottlingException" instead
        case e: AmazonServiceException if e.getErrorCode().startsWith("Throttling") && e.getStatusCode == 400 =>
          log.warn(s"caught exception: ${e.getMessage}\n Retry after 5 seconds...")
          Thread.sleep((Random.nextInt(Math.pow(2, (n + 1)).toInt) + 5) * 1000)
          throttleRetry(func, n + 1)
      }
    else
      func
  }

  def getPipelineId: Option[String]
  def createPipeline(force: Boolean): Option[String]
  def createPipeline(force: Boolean, activate: Boolean): Boolean
  def activatePipeline(): Boolean
  def deletePipeline(): Boolean

}

case class HyperionAwsClientForPipelineId(client: DataPipelineClient, pipelineId: String, maxRetry: Int) extends HyperionAwsClient {

  def getPipelineId: Option[String] = Option(pipelineId)

  def createPipeline(force: Boolean): Option[String] = None

  def createPipeline(force: Boolean, activate: Boolean): Boolean = false

  def deletePipeline(): Boolean = {
    log.info(s"Deleting pipeline $pipelineId")
    throttleRetry(client.deletePipeline(new DeletePipelineRequest().withPipelineId(pipelineId)))
    true
  }

  def activatePipeline(): Boolean = {
    log.info(s"Activating pipeline $pipelineId")
    throttleRetry(client.activatePipeline(new ActivatePipelineRequest().withPipelineId(pipelineId)))
    true
  }

}

case class HyperionAwsClientForName(client: DataPipelineClient, pipelineName: String, maxRetry: Int) extends HyperionAwsClient {

  def getPipelineId: Option[String] = {
    @tailrec
    def queryPipelines(
        ids: List[String] = List.empty,
        request: ListPipelinesRequest = new ListPipelinesRequest()
      ): List[String] = {

      val response = throttleRetry(client.listPipelines(request))

      val theseIds: List[String] = response.getPipelineIdList
        .asScala
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

  def activatePipeline(): Boolean = getPipelineId.map(HyperionAwsClientForPipelineId(client, _, maxRetry)).exists(_.activatePipeline())

  def deletePipeline(): Boolean = getPipelineId.map(HyperionAwsClientForPipelineId(client, _, maxRetry)).exists(_.deletePipeline())

}

case class HyperionAwsClientForPipelineDef(client: DataPipelineClient, pipelineDef: DataPipelineDef) extends HyperionAwsClient {

  lazy val maxRetry = pipelineDef.hc.maxRetry

  def getPipelineId: Option[String] =
    HyperionAwsClientForName(client, pipelineDef.pipelineName, maxRetry).getPipelineId

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
          HyperionAwsClientForPipelineId(client, pipelineId, maxRetry).deletePipeline()
          Thread.sleep(10000) // wait until the data pipeline is really deleted
          createPipeline(force)
        } else {
          log.error("Use --force to force pipeline creation")
          None
        }

      case None =>
        val pipelineId = throttleRetry(
            client.createPipeline(
              new CreatePipelineRequest()
                .withUniqueId(pipelineDef.pipelineName)
                .withName(pipelineDef.pipelineName)
                .withTags(
                  pipelineDef.tags
                    .toSeq
                    .map { case (k, v) => new Tag().withKey(k).withValue(v.getOrElse("")) }
                    .asJava
                )
            )
          ).getPipelineId

        log.info(s"Pipeline created: $pipelineId")
        log.info("Uploading pipeline definition")

        val putDefinitionResult = throttleRetry(
          client.putPipelineDefinition(
            new PutPipelineDefinitionRequest()
              .withPipelineId(pipelineId)
              .withPipelineObjects(pipelineObjects.asJava)
              .withParameterObjects(parameterObjects.asJava)
          )
        )

        putDefinitionResult.getValidationErrors.asScala
          .flatMap(err => err.getErrors.asScala.map(detail => s"${err.getId}: $detail"))
          .foreach(log.error)
        putDefinitionResult.getValidationWarnings.asScala
          .flatMap(err => err.getWarnings.asScala.map(detail => s"${err.getId}: $detail"))
          .foreach(log.warn)

        if (putDefinitionResult.getErrored) {
          log.error("Failed to create pipeline")
          log.error("Deleting the just created pipeline")
          HyperionAwsClientForPipelineId(client, pipelineId, maxRetry).deletePipeline()
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
    if (activate) HyperionAwsClientForPipelineId(client, pipelineId, maxRetry).activatePipeline() else true
  }

  def activatePipeline(): Boolean = getPipelineId.map(HyperionAwsClientForPipelineId(client, _, maxRetry)).exists(_.activatePipeline())

  def deletePipeline(): Boolean = getPipelineId.map(HyperionAwsClientForPipelineId(client, _, maxRetry)).exists(_.deletePipeline())
}

object HyperionAwsClient {

  def getClient(regionId: Option[String] = None, roleArn: Option[String] = None): DataPipelineClient = {
    val region: Region = Region.getRegion(regionId.map(r => Regions.fromName(r)).getOrElse(Regions.US_EAST_1))
    val defaultProvider = new DefaultAWSCredentialsProviderChain()
    val stsProvider = roleArn.map(new STSAssumeRoleSessionCredentialsProvider(defaultProvider, _, "hyperion"))
    new DataPipelineClient(stsProvider.getOrElse(defaultProvider)).withRegion(region)
  }

  def apply(pipelineId: String, regionId: Option[String], roleArn: Option[String], maxRetry: Int): HyperionAwsClient =
    new HyperionAwsClientForPipelineId(getClient(regionId, roleArn), pipelineId, maxRetry)

  def apply(pipelineDef: DataPipelineDef, regionId: Option[String], roleArn: Option[String]): HyperionAwsClient =
    new HyperionAwsClientForPipelineDef(getClient(regionId, roleArn), pipelineDef)

}
