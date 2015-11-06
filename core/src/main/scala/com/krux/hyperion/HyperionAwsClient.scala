package com.krux.hyperion

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import com.amazonaws.auth.{DefaultAWSCredentialsProviderChain, STSAssumeRoleSessionCredentialsProvider}
import com.amazonaws.regions.{Regions, Region}
import com.amazonaws.services.datapipeline._
import com.amazonaws.services.datapipeline.model._
import com.krux.hyperion.DataPipelineDef._

trait HyperionAwsPipelineClient {
  def createPipeline(force: Boolean, activate: Boolean): Boolean
  def activatePipeline(): Boolean
  def deletePipeline(): Boolean
}

class HyperionAwsClient(regionId: Option[String] = None, roleArn: Option[String] = None) {
  lazy val region: Region = Region.getRegion(regionId.map(r => Regions.fromName(r)).getOrElse(Regions.US_EAST_1))
  lazy val defaultProvider = new DefaultAWSCredentialsProviderChain()
  lazy val stsProvider = roleArn.map(new STSAssumeRoleSessionCredentialsProvider(defaultProvider, _, "hyperion"))
  lazy val client: DataPipelineClient = new DataPipelineClient(stsProvider.getOrElse(defaultProvider)).withRegion(region)

  case class ForPipelineId(pipelineId: String) {

    def deletePipelineById(): Boolean = {
      println(s"Deleting pipeline $pipelineId")
      client.deletePipeline(new DeletePipelineRequest().withPipelineId(pipelineId))
      true
    }

    def activatePipelineById(): Boolean = {
      println(s"Activating pipeline $pipelineId")
      client.activatePipeline(new ActivatePipelineRequest().withPipelineId(pipelineId))
      true
    }
  }

  case class ForPipelineDef(pipelineDef: DataPipelineDef) extends HyperionAwsPipelineClient {

    def getPipelineId: Option[String] = {
      @tailrec
      def queryPipelines(ids: List[String] = List.empty, request: ListPipelinesRequest = new ListPipelinesRequest()): List[String] = {
        val response = client.listPipelines(request)

        val theseIds: List[String] = response.getPipelineIdList.collect({
          case idName if idName.getName == pipelineDef.pipelineName => idName.getId
        }).toList

        if (response.getHasMoreResults) {
          queryPipelines(ids ++ theseIds, new ListPipelinesRequest().withMarker(response.getMarker))
        } else {
          ids ++ theseIds
        }
      }

      queryPipelines() match {
        // if using Hyperion for all DataPipeline management, this should never happen
        case _ :: _ :: other => throw new Exception("Duplicated pipeline name")
        case id :: Nil => Option(id)
        case Nil => None
      }
    }

    def createPipeline(force: Boolean, activate: Boolean): Boolean = {
      println(s"Creating pipeline ${pipelineDef.pipelineName}")

      val pipelineObjects: Seq[PipelineObject] = pipelineDef
      val parameterObjects: Seq[ParameterObject] = pipelineDef

      println(s"Pipeline definition has ${pipelineObjects.length} objects")

      getPipelineId match {
        case Some(pipelineId) =>
          println("Pipeline already exists")
          if (force) {
            println("Delete the existing pipeline")
            ForPipelineId(pipelineId).deletePipelineById()
            Thread.sleep(10000)  // wait until the data pipeline is really deleted
            createPipeline(force, activate)
          } else {
            println("Use --force to force pipeline creation")
            false
          }

        case None =>
          val pipelineId = client.createPipeline(
            new CreatePipelineRequest()
              .withUniqueId(pipelineDef.pipelineName)
              .withName(pipelineDef.pipelineName)
              .withTags(pipelineDef.tags.map { case (k, v) => new Tag().withKey(k).withValue(v.getOrElse("")) } )
          ).getPipelineId

          println(s"Pipeline created: $pipelineId")
          println("Uploading pipeline definition")

          val putDefinitionResult = client.putPipelineDefinition(
            new PutPipelineDefinitionRequest()
              .withPipelineId(pipelineId)
              .withPipelineObjects(pipelineObjects)
              .withParameterObjects(parameterObjects)
          )

          putDefinitionResult.getValidationErrors.flatMap(_.getErrors.map(e => s"ERROR: $e")).foreach(println)
          putDefinitionResult.getValidationWarnings.flatMap(_.getWarnings.map(e => s"WARNING: $e")).foreach(println)

          if (putDefinitionResult.getErrored) {
            println("Failed to create pipeline")
            println("Deleting the just created pipeline")
            ForPipelineId(pipelineId).deletePipelineById()
          } else if (putDefinitionResult.getValidationErrors.isEmpty
            && putDefinitionResult.getValidationWarnings.isEmpty) {
            println("Successfully created pipeline")
            if (activate) ForPipelineId(pipelineId).activatePipelineById() else true
          } else {
            println("Successful with warnings")
            if (activate) ForPipelineId(pipelineId).activatePipelineById() else true
          }
      }
    }

    def activatePipeline(): Boolean = pipelineNameAction().exists(_.activatePipelineById())

    def deletePipeline(): Boolean = pipelineNameAction().exists(_.deletePipelineById())

    private def pipelineNameAction(): Option[ForPipelineId] = getPipelineId match {
      case Some(pipelineId) =>
        Option(ForPipelineId(pipelineId))

      case None =>
        println(s"Pipeline ${pipelineDef.pipelineName} does not exist")
        None
    }

  }

}
