package com.krux.hyperion.activity

trait RedshiftCopyOption {
  def repr: Seq[String]
}

object RedshiftCopyOption {

  def csv(quote: String) = new RedshiftCopyOption {
    def repr = Seq("CSV", "QUOTE", quote)
  }

  def csv = new RedshiftCopyOption {
    def repr = Seq("CSV")
  }

  def gzip = new RedshiftCopyOption {
    def repr = Seq("GZIP")
  }

  def delimiter(delChar: String) = new RedshiftCopyOption {
    def repr = Seq("DELIMITER", s"'$delChar'")
  }

  def escape = new RedshiftCopyOption {
    def repr = Seq("ESCAPE")
  }

  def nullAs(nullStr: String) = new RedshiftCopyOption {
    def repr = Seq("NULL", s"'$nullStr'")
  }

  def maxError(errorCount: Int) = new RedshiftCopyOption {
    def repr = Seq("MAXERROR", errorCount.toString)
  }

  def acceptInvChars = new RedshiftCopyOption {
    def repr = Seq("ACCEPTINVCHARS")
  }

  def acceptInvChars(replacementChar: Char) = new RedshiftCopyOption {
    def repr = Seq("ACCEPTINVCHARS", s"'$replacementChar'")
  }

  def acceptAnyDate = new RedshiftCopyOption {
    def repr = Seq("ACCEPTANYDATE")
  }

  def blanksAsNull = new RedshiftCopyOption {
    def repr = Seq("BLANKSASNULL")
  }

  def dateFormat = new RedshiftCopyOption {
    def repr = Seq("DATEFORMAT")
  }

  def dateFormat(dateFormatString: String) = new RedshiftCopyOption {
    def repr = Seq("DATEFORMAT", s"'$dateFormatString'")
  }

  def encoding(fileEncoding: String) = new RedshiftCopyOption {
    def repr = Seq("ENCODING", fileEncoding)
  }

  def explicitIds = new RedshiftCopyOption {
    def repr = Seq("EXPLICIT_IDS")
  }

  def fillRecord = new RedshiftCopyOption {
    def repr = Seq("FILLRECORD")
  }

  def ignoreBlankLines = new RedshiftCopyOption {
    def repr = Seq("IGNOREBLANKLINES")
  }

  def ignoreHeader(numberRows: Long) = new RedshiftCopyOption {
    def repr = Seq("IGNOREHEADER", numberRows.toString)
  }

  def removeQuotes = new RedshiftCopyOption {
    def repr = Seq("REMOVEQUOTES")
  }

  def roundec = new RedshiftCopyOption {
    def repr = Seq("ROUNDEC")
  }

  def timeFormat(format: String) = new RedshiftCopyOption {
    def repr = Seq("TIMEFORMAT", s"'$format'")
  }

  def trimBlanks = new RedshiftCopyOption {
    def repr = Seq("TRIMBLANKS")
  }

  def truncateColumns = new RedshiftCopyOption {
    def repr = Seq("TRUNCATECOLUMNS")
  }
}
