package com.krux.hyperion.activity

import com.krux.hyperion.adt.{ HString, HBoolean }
import com.krux.hyperion.datanode.S3DataNode

case class ShellCommandActivityFields(
  script: Script,
  scriptArguments: Seq[HString] = Seq.empty,
  stdout: Option[HString] = None,
  stderr: Option[HString] = None,
  stage: Option[HBoolean] = None,
  input: Seq[S3DataNode] = Seq.empty,
  output: Seq[S3DataNode] = Seq.empty
)
