package com.krux.hyperion.workflow

import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._
import com.amazonaws.services.datapipeline.model.PipelineObject
import com.krux.hyperion.DataPipelineDef

import scala.util.Try

case class WorkflowGraphRenderer(
  pipeline: DataPipelineDef,
  removeLastNameSegment: Boolean,
  includeResources: Boolean
) {
  private lazy val styles = ConfigFactory.load("reference").getConfig("hyperion.graphviz.styles")

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
    val attrs = getAttributes(s"${idToTypeMap(from)}To${idToTypeMap(to)}", styles)
      .orElse(getAttributes(s"${idToTypeMap(from)}ToAny", styles))
      .orElse(getAttributes(s"AnyTo${idToTypeMap(to)}", styles))
    s"  ${quoted(from)} -> ${quoted(to)} ${attrs.getOrElse("")}"
  }

  def render(): String = {
    val parts = Seq(s"strict digraph ${quoted(pipeline.pipelineName)} {") ++
    pipelineObjects.flatMap { obj =>
      obj.getFields.asScala.flatMap { field =>
        field.getKey match {
          case "type" =>
            getAttributes(field.getStringValue, styles).map(renderNode(obj.getId, _))

          case "output" =>
            Option(renderEdge(obj.getId, field.getRefValue))

          case "input" | "dependsOn" =>
            Option(renderEdge(field.getRefValue, obj.getId))

          case "runsOn" if includeResources =>
            Option(renderEdge(field.getRefValue, obj.getId))

          case _ => None
        }

      }
    } ++
    Seq("}", "")

    parts.mkString("\n")
  }
}
