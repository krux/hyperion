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

}
