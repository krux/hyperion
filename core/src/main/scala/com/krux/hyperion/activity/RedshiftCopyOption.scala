package com.krux.hyperion.activity

case class RedshiftCopyOption(repr: Seq[String])

object RedshiftCopyOption {

  def csv(quote: String) = RedshiftCopyOption(Seq("CSV", "QUOTE", quote))

  def csv = RedshiftCopyOption(Seq("CSV"))

  def gzip = RedshiftCopyOption(Seq("GZIP"))

  def delimiter(delChar: String) = RedshiftCopyOption(Seq("DELIMITER", s"'$delChar'"))

  def escape = RedshiftCopyOption(Seq("ESCAPE"))

  def nullAs(nullStr: String) = RedshiftCopyOption(Seq("NULL", s"'$nullStr'"))

  def maxError(errorCount: Int) = RedshiftCopyOption(Seq("MAXERROR", errorCount.toString))

  def acceptInvChars = RedshiftCopyOption(Seq("ACCEPTINVCHARS"))

  def acceptInvChars(replacementChar: Char) =
    RedshiftCopyOption(Seq("ACCEPTINVCHARS", s"'$replacementChar'"))

  def acceptAnyDate = RedshiftCopyOption(Seq("ACCEPTANYDATE"))

  def blanksAsNull = RedshiftCopyOption(Seq("BLANKSASNULL"))

  def dateFormat = RedshiftCopyOption(Seq("DATEFORMAT"))

  def dateFormat(dateFormatString: String) = RedshiftCopyOption(Seq("DATEFORMAT", s"'$dateFormatString'"))

  def encoding(fileEncoding: String) = RedshiftCopyOption(Seq("ENCODING", fileEncoding))

  def explicitIds = RedshiftCopyOption(Seq("EXPLICIT_IDS"))

  def fillRecord = RedshiftCopyOption(Seq("FILLRECORD"))

  def ignoreBlankLines = RedshiftCopyOption(Seq("IGNOREBLANKLINES"))

  def ignoreHeader(numberRows: Long) = RedshiftCopyOption(Seq("IGNOREHEADER", numberRows.toString))

  def removeQuotes = RedshiftCopyOption(Seq("REMOVEQUOTES"))

  def roundec = RedshiftCopyOption(Seq("ROUNDEC"))

  def timeFormat(format: String) = RedshiftCopyOption(Seq("TIMEFORMAT", s"'$format'"))

  def trimBlanks = RedshiftCopyOption(Seq("TRIMBLANKS"))

  def truncateColumns = RedshiftCopyOption(Seq("TRUNCATECOLUMNS"))
}
