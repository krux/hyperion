package com.krux.hyperion.h3.precondition

import com.krux.hyperion.adt.{ HDuration, HString }

case class PreconditionFields(role: HString, preconditionTimeout: Option[HDuration] = None)
