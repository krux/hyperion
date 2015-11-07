package com.krux.hyperion.cli

import java.io.File

import com.krux.hyperion.Schedule

case class Options(
  action: Action = GenerateAction,
  activate: Boolean = false,
  force: Boolean = false,
  pipelineId: Option[String] = None,
  customName: Option[String] = None,
  tags: Map[String, Option[String]] = Map.empty,
  schedule: Option[Schedule] = None,
  region: Option[String] = None,
  roleArn: Option[String] = None,
  output: Option[File] = None,
  label: String = "id",
  removeLastNameSegment: Boolean = false,
  includeResources: Boolean = false,
  includeDataNodes: Boolean = false,
  includeDatabases: Boolean = false
)
