package com.krux.hyperion.workflow

import scala.collection.JavaConverters._
import scala.util.Try

import com.amazonaws.services.datapipeline.model.PipelineObject
import com.krux.hyperion.DataPipelineDef
import com.typesafe.config.Config

case class WorkflowGraphRenderer(
  pipeline: DataPipelineDef,
  removeLastNameSegment: Boolean,
  label: String,
  includeResources: Boolean,
  includeDataNodes: Boolean,
  includeDatabases: Boolean
) {

  private lazy val pipelineObjects: Iterable[PipelineObject] = pipeline.toAwsPipelineObjects.values.flatten

  private lazy val idToTypeMap: Map[String, String] = pipelineObjects.flatMap { obj =>
    obj.getFields.asScala.find(_.getKey == "type").map(f => obj.getId -> f.getStringValue)
  }.toMap

  private lazy val idToLabelMap: Map[String, String] = pipelineObjects.flatMap { obj =>
    label match {
      case "id" => Option(obj.getId -> stripId(obj.getId))
      case "name" => Option(obj.getId -> obj.getName)
      case _ => obj.getFields.asScala.find(_.getKey == label).map(f => obj.getId -> Option(f.getStringValue).getOrElse(stripId(obj.getId)))
    }
  }.toMap

  private def stripId(id: String) = if (removeLastNameSegment) id.split('_').dropRight(1).mkString("_") else id

  private def quoted(s: String) = s""""$s""""

  private def getLabel(id: String) = quoted(idToLabelMap.getOrElse(id, stripId(id)) match {
    case "" => id
    case idLabel => idLabel
  })

  private def getAttributes(which: String, where: Config): Option[String] =
    Try(where.getObject(which)).toOption.map { conf =>
      "[" + conf.unwrapped().asScala.map { case (k, v) => s"$k=$v" }.mkString(", ") + "]"
    }

  private def renderNode(id: String, attrs: String) = s"  ${getLabel(id)} $attrs\n"

  private def renderEdge(from: String, to: String) = {
    val attrs = getAttributes(s"${idToTypeMap(from)}To${idToTypeMap(to)}", pipeline.hc.graphStyles)
      .orElse(getAttributes(s"${idToTypeMap(from)}ToAny", pipeline.hc.graphStyles))
      .orElse(getAttributes(s"AnyTo${idToTypeMap(to)}", pipeline.hc.graphStyles))
    s"  ${getLabel(from)} -> ${getLabel(to)} ${attrs.getOrElse("")}"
  }

  private def ifNodeIncluded(nodeType: String)(func: => String): Option[String] = nodeType match {
    case "Ec2Resource" | "EmrCluster" if !includeResources => None
    case "DynamoDBDataNode" | "MySqlDataNode" | "RedshiftDataNode" | "S3DataNode" | "SqlDataNode" if !includeDataNodes => None
    case "JdbcDatabase" | "RdsDatabase" | "RedshiftDatabase" if !includeDatabases => None
    case _ => Option(func)
  }

  def render(): String = {
    val parts = Seq(
      s"strict digraph ${quoted(pipeline.pipelineName)} {"
    ) ++ pipelineObjects.flatMap { obj =>
      obj.getFields.asScala.flatMap { field =>
        field.getKey match {
          case "type" =>
            getAttributes(field.getStringValue, pipeline.hc.graphStyles).flatMap { attrs =>
              ifNodeIncluded(field.getStringValue) {
                renderNode(obj.getId, attrs)
              }
            }

          case "output" if includeDataNodes =>
            Option(renderEdge(obj.getId, field.getRefValue))

          case "input" if includeDataNodes =>
            Option(renderEdge(field.getRefValue, obj.getId))

          case "dependsOn" =>
            Option(renderEdge(field.getRefValue, obj.getId))

          case "runsOn" if includeResources =>
            Option(renderEdge(field.getRefValue, obj.getId))

          case _ => None
        }

      }
    } ++ Seq(
      "}",
      ""
    )

    parts.mkString("\n")
  }
}
