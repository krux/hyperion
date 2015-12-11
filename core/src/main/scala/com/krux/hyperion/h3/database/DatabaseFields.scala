package com.krux.hyperion.h3.database

import com.krux.hyperion.adt.HString

case class DatabaseFields(
  username: HString,
  `*password`: HString,
  databaseName: Option[HString] = None
)
