package com.krux.hyperion

import scala.language.implicitConversions

import java.time.ZonedDateTime

import com.krux.hyperion.common.S3Uri
import com.krux.hyperion.expression.Duration

package object adt {

  // somehow the following does not work in all situations:
  // implicit def seqNative2SeqHType[B <: HType, A <% B](x: Seq[A]): Seq[B] = x.map(xx => xx: B)

  // since the generic one does not work we have to write all supported ones explicitly
  implicit def seqString2SeqHString(x: Seq[String]): Seq[HString] = x.map(xx => xx: HString)
  implicit def seqInt2SeqHInt(x: Seq[Int]): Seq[HInt] = x.map(xx => xx: HInt)
  implicit def seqDouble2SeqHDouble(x: Seq[Double]): Seq[HDouble] = x.map(xx => xx: HDouble)
  implicit def seqBoolean2SeqHBoolean(x: Seq[Boolean]): Seq[HBoolean] = x.map(xx => xx: HBoolean)
  implicit def seqDateTime2SeqHDateTime(x: Seq[ZonedDateTime]): Seq[HDateTime] = x.map(xx => xx: HDateTime)
  implicit def seqDuration2SeqHDuration(x: Seq[Duration]): Seq[HDuration] = x.map(xx => xx: HDuration)
  implicit def seqS3Uri2SeqHS3Uri(x: Seq[S3Uri]): Seq[HS3Uri] = x.map(xx => xx: HS3Uri)
  implicit def seqLong2SeqHLong(x: Seq[Long]): Seq[HLong] = x.map(xx => xx: HLong)

}
