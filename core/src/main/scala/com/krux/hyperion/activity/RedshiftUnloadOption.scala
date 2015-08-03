package com.krux.hyperion.activity

trait RedshiftUnloadOption {
  def repr: Seq[String]
}

object RedshiftUnloadOption {

  def gzip = new RedshiftUnloadOption {
    def repr = Seq("GZIP")
  }

  def delimiter(delChar: String) = new RedshiftUnloadOption {
    def repr = Seq("DELIMITER", s"'$delChar'")
  }

  def allowOverwrite = new RedshiftUnloadOption {
    def repr = Seq("ALLOWOVERWRITE")
  }

  def escape = new RedshiftUnloadOption {
    def repr = Seq("ESCAPE")
  }

}
