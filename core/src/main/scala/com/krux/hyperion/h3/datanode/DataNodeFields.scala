package com.krux.hyperion.h3.datanode

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.precondition.Precondition

case class DataNodeFields (
  precondition: Seq[Precondition] = Seq.empty,
  onFailAlarms: Seq[SnsAlarm] = Seq.empty,
  onSuccessAlarms: Seq[SnsAlarm] = Seq.empty
)
