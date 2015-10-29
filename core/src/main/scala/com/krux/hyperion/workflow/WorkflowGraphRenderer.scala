package com.krux.hyperion.workflow

import scala.util.Try
import scala.collection.JavaConverters._

import com.typesafe.config.Config

import com.amazonaws.services.datapipeline.model.PipelineObject
import com.krux.hyperion.DataPipelineDef

case class WorkflowGraphRenderer(
  pipeline: DataPipelineDef,
  removeLastNameSegment: Boolean,
  includeResources: Boolean
) {
  private lazy val pipelineObjects: Seq[PipelineObject] = pipeline

  private lazy val idToTypeMap: Map[String, String] = pipelineObjects.flatMap { obj =>
    obj.getFields.asScala.find(_.getKey == "type").map(f => obj.getId -> f.getStringValue)
  }.toMap

  private def quoted(s: String) = if (removeLastNameSegment) s""""${s.split('_').dropRight(1).mkString("_")}"""" else s""""$s""""

  private def getAttributes(which: String, where: Config): Option[String] =
    Try(where.getObject(which)).toOption.map { conf =>
      "[" + conf.unwrapped().asScala.map { case (k, v) => s"$k=$v" }.mkString(", ") + "]"
    }

  private def renderNode(id: String, attrs: String) = s"  ${quoted(id)} $attrs\n"

  private def renderEdge(from: String, to: String) = {
    val attrs = getAttributes(s"${idToTypeMap(from)}To${idToTypeMap(to)}", pipeline.hc.graphStyles)
      .orElse(getAttributes(s"${idToTypeMap(from)}ToAny", pipeline.hc.graphStyles))
      .orElse(getAttributes(s"AnyTo${idToTypeMap(to)}", pipeline.hc.graphStyles))
    s"  ${quoted(from)} -> ${quoted(to)} ${attrs.getOrElse("")}"
  }

  def render(): String = {
    val parts = Seq(
      s"strict digraph ${quoted(pipeline.pipelineName)} {"
    ) ++ pipelineObjects.flatMap { obj =>
      obj.getFields.asScala.flatMap { field =>
        field.getKey match {
          case "type" =>
            getAttributes(field.getStringValue, pipeline.hc.graphStyles).map(renderNode(obj.getId, _))

          case "output" =>
            Option(renderEdge(obj.getId, field.getRefValue))

          case "input" | "dependsOn" =>
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
