package com.krux.hyperion.cli

import java.io.File

import com.krux.hyperion.Schedule

trait Options {
  def activate: Boolean
  def force: Boolean
  def pipelineId: Option[String]
  def customName: Option[String]
  def tags: Map[String, Option[String]]
  def schedule: Option[Schedule]
  def region: Option[String]
  def roleArn: Option[String]
  def output: Option[File]
  def label: String
  def removeLastNameSegment: Boolean
  def includeResources: Boolean
  def includeDataNodes: Boolean
  def includeDatabases: Boolean
}

case class Cli(
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
) extends Options
