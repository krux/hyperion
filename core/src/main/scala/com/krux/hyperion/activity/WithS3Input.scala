package com.krux.hyperion.activity

import com.krux.hyperion.adt.HBoolean
import com.krux.hyperion.datanode.S3DataNode

trait WithS3Input {

  type Self <: WithS3Input

  def shellCommandActivityFields: ShellCommandActivityFields
  def updateShellCommandActivityFields(fields: ShellCommandActivityFields): Self

  def withInput(inputs: S3DataNode*): Self = updateShellCommandActivityFields(
    shellCommandActivityFields.copy(
      input = shellCommandActivityFields.input ++ inputs,
      stage = Option(HBoolean.True)
    )
  )

}
