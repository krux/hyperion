package com.krux.hyperion.io

import scala.annotation.tailrec
import scala.collection.JavaConverters._

import com.amazonaws.services.datapipeline.DataPipelineClient
import com.amazonaws.services.datapipeline.model.ListPipelinesRequest

import com.krux.hyperion.AbstractDataPipelineDef


case class AwsClientForName(
    client: DataPipelineClient,
    pipelineName: String,
    nameKeySeparator: String = AbstractDataPipelineDef.DefaultNameKeySeparator
  ) extends AwsClient {

  lazy val pipelineIdNames: Map[String, String] = getPipelineIdNames()

  /**
   * TODO: Note this is copied from AwsClientForDef with the consistency name check removed
   */
  private def getPipelineIdNames(): Map[String, String] = {

    def inGroup(actualName: String): Boolean = (
      actualName == pipelineName ||
      actualName.startsWith(pipelineName + nameKeySeparator)
    )

    @tailrec
    def queryPipelines(
        idNames: Map[String, String] = Map.empty,
        request: ListPipelinesRequest = new ListPipelinesRequest()
      ): Map[String, String] = {

      val response = throttleRetry(client.listPipelines(request))
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
      if (names.size != idNames.size) throw new RuntimeException("Duplicated pipeline name")

      if (idNames.isEmpty) log.debug(s"Pipeline ${pipelineName} does not exist")

    }

    val result = queryPipelines()
    checkResults(result)
    result

  }

  def forId(): Option[AwsClientForId] =
    if (pipelineIdNames.isEmpty) None
    else Option(AwsClientForId(client, pipelineIdNames.keySet))

}