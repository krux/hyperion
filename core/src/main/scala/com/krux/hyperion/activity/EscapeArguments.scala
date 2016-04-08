package com.krux.hyperion.activity

import com.krux.hyperion.adt.HString

/**
  * Escape ',' in arguments used in MapReduceStep and SparkStep
  */
trait EscapeArguments {

  def args: Seq[HString]

  def escapedArguments: Seq[HString] = args.map {
    case HString(Left(s)) => HString(Left(s.replaceAll("," , "\\\\,")))
    case other => other
  }

}
