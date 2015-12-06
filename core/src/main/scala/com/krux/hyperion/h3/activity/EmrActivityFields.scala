package com.krux.hyperion.h3.activity

import com.krux.hyperion.adt.HString

case class EmrActivityFields(
  preStepCommands: Seq[HString] = Seq.empty,
  postStepCommands: Seq[HString] = Seq.empty
)
