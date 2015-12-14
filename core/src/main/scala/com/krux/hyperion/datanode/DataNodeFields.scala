package com.krux.hyperion.datanode

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.precondition.Precondition

case class DataNodeFields (
  preconditions: Seq[Precondition] = Seq.empty,
  onFailAlarms: Seq[SnsAlarm] = Seq.empty,
  onSuccessAlarms: Seq[SnsAlarm] = Seq.empty
)
