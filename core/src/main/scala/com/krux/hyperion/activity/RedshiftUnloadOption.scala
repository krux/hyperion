package com.krux.hyperion.activity

trait RedshiftUnloadOption {
  def repr: Seq[String]
}

object RedshiftUnloadOption {

  def manifest = new RedshiftUnloadOption {
    def repr = Seq("MANIFEST")
  }

  def delimiter(delChar: String) = new RedshiftUnloadOption {
    def repr = Seq("DELIMITER", s"'$delChar'")
  }

  def fixedwidth(fixewidthSpec: String) = new RedshiftUnloadOption {
    def repr = Seq("FIXEDWIDTH", s"'$fixewidthSpec'")
  }

  def encrypted = new RedshiftUnloadOption {
    def repr = Seq("ENCRYPTED")
  }

  def bzip2 = new RedshiftUnloadOption {
    def repr = Seq("BZIP2")
  }

  def gzip = new RedshiftUnloadOption {
    def repr = Seq("GZIP")
  }

  def addQuotes = new RedshiftUnloadOption {
    def repr = Seq("ADDQUOTES")
  }

  def nullAs(nullString: String) = new RedshiftUnloadOption {
    def repr = Seq("NULL", s"'$nullString'")
  }

  def escape = new RedshiftUnloadOption {
    def repr = Seq("ESCAPE")
  }

  def allowOverwrite = new RedshiftUnloadOption {
    def repr = Seq("ALLOWOVERWRITE")
  }

  def parallel(on: Boolean) = new RedshiftUnloadOption {
    def repr = Seq("PARALLEL", if (on) "ON" else "OFF")
  }

}
