package com.krux.hyperion.h3.activity

import com.krux.hyperion.adt.HString

case class EmrActivityFields[A <: EmrStep](
  preStepCommands: Seq[HString],
  postStepCommands: Seq[HString],
  steps: Set[A]
)
