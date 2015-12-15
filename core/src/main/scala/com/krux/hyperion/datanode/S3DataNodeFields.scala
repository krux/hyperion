package com.krux.hyperion.datanode

import com.krux.hyperion.dataformat.DataFormat
import com.krux.hyperion.adt.{ HS3Uri, HBoolean }

case class S3DataNodeFields(
  dataFormat: Option[DataFormat] = None,
  manifestFilePath: Option[HS3Uri] = None,
  isEncrypted: HBoolean = HBoolean.True,  // server encryption is enabled by default
  isCompressed: HBoolean = HBoolean.False
)
