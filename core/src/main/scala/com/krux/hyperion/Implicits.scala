package com.krux.hyperion

import scala.language.implicitConversions

import com.github.nscala_time.time.Imports._
import com.krux.hyperion.common.Memory
import com.krux.hyperion.common.S3Uri.S3StringContext
import com.krux.hyperion.common.HdfsUri.HdfsStringContext
import com.krux.hyperion.expression._
import org.json4s.DefaultFormats

/**
 * The implicit conversions used in DataPipeline
 */
object Implicits {

  implicit val jsonFormats = DefaultFormats

  implicit def string2DateTime(day: String): DateTime = new DateTime(day)

  implicit class DurationBuilder(n: Int) {
    def year = Year(n)
    def years = this.year

    def month = Month(n)
    def months = this.month

    def week = Week(n)
    def weeks = this.week

    def day = Day(n)
    def days = this.day

    def hour = Hour(n)
    def hours = this.hour
  }

  implicit def stringContext2S3UriHelper(sc: StringContext): S3StringContext = S3StringContext(sc)

  implicit def stringContext2SHdfsUriHelper(sc: StringContext): HdfsStringContext = HdfsStringContext(sc)

  implicit class Int2Memory(n: Int) {
    def kilobytes = Memory(n, "K")
    def megabytes = Memory(n, "M")
    def gigabytes = Memory(n, "G")
  }

  implicit class Long2Memory(n: Long) {
    def kilobytes = Memory(n, "K")
    def megabytes = Memory(n, "M")
    def gigabytes = Memory(n, "G")
  }

}
