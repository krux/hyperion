package com.krux.hyperion.database

import com.krux.hyperion.adt.HString

case class DatabaseFields(
  username: HString,
  `*password`: HString,
  databaseName: Option[HString] = None
)
