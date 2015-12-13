package com.krux.hyperion.precondition

import com.krux.hyperion.adt.{ HDuration, HString }

case class PreconditionFields(role: HString, preconditionTimeout: Option[HDuration] = None)
