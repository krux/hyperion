package com.krux.hyperion

import scala.annotation.tailrec
import scala.collection.JavaConverters._

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.datapipeline.DataPipelineClient
import com.amazonaws.services.datapipeline.model.{ PipelineObject, ListPipelinesRequest,
  ParameterObject, CreatePipelineRequest, Tag }
import org.slf4j.LoggerFactory


trait PipelineGroupAwsClient {

  lazy val log = LoggerFactory.getLogger(getClass)

  // def pipelineIdNames: Map[String, String]

}

case class PipelineGroupAwsClientForIds(
    awsClient: DataPipelineClient,
    // pipelineIdNames: Map[String, String]
    pipelineIds: Set[String]
  ) extends PipelineGroupAwsClient {

  def activatePipelines(): Option[PipelineGroupAwsClientForIds] = ???

  def deletePipelines(): Unit = ???

}

case class PipelineGroupAwsClientForDef(
    awsClient: DataPipelineClient,
    pipelineDef: AbstractDataPipelineDef
  ) extends PipelineGroupAwsClient {

  lazy val pipelineObjects: Map[WorkflowKey, Seq[PipelineObject]] = pipelineDef.toAwsPipelineObjects

  lazy val parameterObjects: Seq[ParameterObject] = pipelineDef.toAwsParameters

  private def createEmptyPipelines(force: Boolean): Option[PipelineGroupAwsClientForDef] = {
    val existingPipelines = getPipelineIdNames()

    if (existingPipelines.nonEmpty)
    ???
  }

  private def uploadDefinition(): Option[PipelineGroupAwsClientForIds] = ???

  def createPipelines(force: Boolean): Option[PipelineGroupAwsClientForIds] = {

    log.info(s"Creating pipline ${pipelineDef.pipelineName}")

    createEmptyPipelines(force).flatMap(_.uploadDefinition())

    // if (existingPipelines.nonEmpty) {
    //   log.warn("Pipeline group already exists")

    //   if (force) {
    //     log.info("Delete the existing pipeline")
    //     PipelineGroupAwsClientForIds(awsClient, existingPipelines.keySet).deletePipelines()
    //     Thread.sleep(10000) // wait until the data pipeline is really deleted
    //     createPipelines(force)
    //   } else {
    //     log.error("Use --force to force pipeline creation")
    //     None
    //   }

    // } else {

    //   val deployedPipelineIds = pipelineObjects
    //     .foldLeft(Set.empty[String]) { case (ids, (wfKey, pipelineObjects)) =>
    //       val createdPipeline = throttleRetry(
    //         awsClient.createPipeline(
    //           new CreatePipelineRequest()
    //             .withUniqueId(pipelineDef.pipelineName)
    //             .withName(pipelineDef.pipelineName)
    //             .withTags(
    //               pipelineDef.tags.toSeq
    //                 .map { case (k, v) => new Tag().withKey(k).withValue(v.getOrElse("")) }
    //                 .asJava
    //             )
    //         )
    //       )

    //       ids + createdPipeline.getPipelineId
    //     }

    //   if (deployedPipelineIds.size != pipelineObjects.size) {
    //     log.error("Created pipelines does not match definition, cleaning up...")
    //     PipelineGroupAwsClientForIds(awsClient, deployedPipelineIds).deletePipelines()
    //     None
    //   } else {
    //     uploadeDefinition()
    //   }

    // }

  }

  def activatePipelines(): Boolean = ???

  private def throttleRetry[A](func: => A, n: Int = 3): A = {
    if (n > 0)
      try {
        func
      } catch {
        // use startsWith becase the doc says the error code is called "Throttling" but sometimes
        // we see "ThrottlingException" instead
        case e: AmazonServiceException if e.getErrorCode().startsWith("Throttling") && e.getStatusCode == 400 =>
          log.warn(s"caught exception: ${e.getMessage}\n Retry after 5 seconds...")
          Thread.sleep(5000)
          throttleRetry(func, n - 1)
      }
    else
      func
  }

  /**
   * Retrive the pipelines IDs and Names based on the master name defined in the pipeline def
   */
  private def getPipelineIdNames(): Map[String, String] = {

    val pipelineMasterName = pipelineDef.pipelineName

    def inGroup(actualName: String): Boolean = (
      actualName == pipelineMasterName ||
      actualName.startsWith(pipelineMasterName + pipelineDef.NameKeySeparator)
    )

    @tailrec
    def queryPipelines(
        idNames: Map[String, String] = Map.empty,
        request: ListPipelinesRequest = new ListPipelinesRequest()
      ): Map[String, String] = {

      val response = throttleRetry(awsClient.listPipelines(request))
      val theseIdNames = response.getPipelineIdList
        .asScala
        .collect { case idName if inGroup(idName.getName) => (idName.getId, idName.getName) }
        .toMap

      if (response.getHasMoreResults)
        queryPipelines(
          idNames ++ theseIdNames,
          new ListPipelinesRequest().withMarker(response.getMarker)
        )
      else
        idNames ++ theseIdNames

    }

    def checkResults(idNames: Map[String, String]): Unit = {
      val names = idNames.values.toSet

      // if using Hyperion for all DataPipeline management, this should never happen
      if (names.size != idNames.size)
        throw new RuntimeException("Duplicated pipeline name")

      if (names != pipelineDef.pipelineNames)
        log.warn("inconsistent data pipline names")

      if (idNames.isEmpty)
        log.debug(s"Pipeline ${pipelineMasterName} does not exist")

    }

    val result = queryPipelines()
    checkResults(result)
    result
  }

}

object PipelineGroupAwsClient {
}
