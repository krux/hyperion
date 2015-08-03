package com.krux.hyperion

import com.amazonaws.auth.{DefaultAWSCredentialsProviderChain, STSAssumeRoleSessionCredentialsProvider}
import com.amazonaws.regions.{Regions, Region}
import com.amazonaws.services.datapipeline._
import com.amazonaws.services.datapipeline.model._
import com.krux.hyperion.DataPipelineDef._
import scala.collection.JavaConversions._

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

  case class ForPipelineDef(
    pipelineDef: DataPipelineDef,
    customName: Option[String] = None
  ) {

    val pipelineName = customName.getOrElse(pipelineDef.pipelineName)

    def getPipelineId: Option[String] = {
      val idNameToIdPartial = Function.unlift((idName: PipelineIdName) =>
        if (idName.getName == pipelineName) Option(idName.getId) else None
      )

      client.listPipelines().getPipelineIdList.collect(idNameToIdPartial).toList match {
        case x :: y :: other =>  // if using hyperion for all DataPipeline management, this should never happen
          throw new Exception("Duplicated pipeline name")
        case x :: Nil => Option(x)
        case Nil => None
      }
    }

    def createPipeline(force: Boolean = false, tags: Map[String, Option[String]] = Map()): Option[String] = {
      println(s"Creating pipeline $pipelineName")

      getPipelineId match {
        case Some(pipelineId) =>
          println("Pipeline already exists")
          if (force) {
            println("Delete the existing pipline")
            ForPipelineId(pipelineId).deletePipelineById()
            Thread.sleep(10000)  // wait until the data pipeline is really deleted
            createPipeline(force = true, tags)
          } else {
            println("Use --force to force pipeline creation")
            None
          }

        case None =>
          val pipelineId = client.createPipeline(
            new CreatePipelineRequest()
              .withTags((pipelineDef.tags ++ tags).map { case (k, v) => new Tag().withKey(k).withValue(v.getOrElse("")) } )
              .withUniqueId(pipelineName)
              .withName(pipelineName)
          ).getPipelineId

          println(s"Pipeline created: $pipelineId")
          println("Uploading pipeline definition")

          val putDefinitionResult = client.putPipelineDefinition(
            new PutPipelineDefinitionRequest()
              .withPipelineId(pipelineId)
              .withPipelineObjects(pipelineDef: Seq[PipelineObject])
              .withParameterObjects(pipelineDef: Seq[ParameterObject])
          )

          putDefinitionResult.getValidationErrors.flatMap(_.getErrors.map(e => s"ERROR: $e")).foreach(println)
          putDefinitionResult.getValidationWarnings.flatMap(_.getWarnings.map(e => s"WARNING: $e")).foreach(println)

          if (putDefinitionResult.getErrored) {
            println("Failed to create pipeline")
            println("Deleting the just created pipeline")
            ForPipelineId(pipelineId).deletePipelineById()
            None
          } else if (putDefinitionResult.getValidationErrors.isEmpty
            && putDefinitionResult.getValidationWarnings.isEmpty) {
            println("Successfully created pipeline")
            Option(pipelineId)
          } else {
            println("Successful with warnings")
            Option(pipelineId)
          }
      }
    }

    private def pipelineNameAction(): Option[ForPipelineId] =
      getPipelineId match {
        case Some(pipelineId) =>
          Option(ForPipelineId(pipelineId))

        case None =>
          println(s"Pipeline $pipelineName does not exist")
          None
      }

    def activatePipeline(): Boolean = pipelineNameAction().exists(_.activatePipelineById())

    def deletePipeline(): Boolean = pipelineNameAction().exists(_.deletePipelineById())

  }

}
