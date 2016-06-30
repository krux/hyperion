package com.krux.hyperion.common

import scala.annotation.tailrec


trait Escapable {

  /**
   * Given the start of exp, seek the end of expression returning the expression block and the rest
   * of the string. Note that expression is not a nested structure and the only legitimate '{' or
   * '}' within a expression is within quotes (i.e. '"' or "'")
   *
   * @note this does not handle the case that expression have escaped quotes (i.e. "\"" or '\'')
   */
  @tailrec
  private def seekEndOfExpr(
    exp: String,
    quote: Option[Char] = None,
    expPart: StringBuilder = StringBuilder.newBuilder
  ): (String, String) = {

    if (exp.isEmpty) {
      throw new RuntimeException("Expression started but not ended")
    } else {
      val curChar = exp.head
      val next = exp.tail

      quote match {
        case Some(quoteChar) => // if is in quote
          seekEndOfExpr(next, quote.filter(_ != curChar), expPart += curChar)
        case _ =>
          curChar match {
            case '}' => ((expPart += curChar).result, next)
            case '\'' | '"' => seekEndOfExpr(next, Option(curChar), expPart += curChar)
            case _ => seekEndOfExpr(next, None, expPart += curChar)
          }
      }
    }
  }

  def escape(s: String, c: Char): String = {

    def escapeChar(cc: Char): String = if (cc == c) s"\\\\$c" else cc.toString

    @tailrec
    def escapeRec(
      s: String,
      hashSpotted: Boolean = false,
      result: StringBuilder = StringBuilder.newBuilder
    ): String = {

      if (s.isEmpty) {
        result.toString
      } else {
        val curChar = s.head
        val sTail = s.tail

        if (!hashSpotted) {  // outside an expression block
          escapeRec(sTail, curChar == '#', result ++= escapeChar(curChar))
        } else {  // the previous char is '#'
          if (curChar == '{') {  // start of an expression
            val (blockBody, rest) = seekEndOfExpr(sTail)
            escapeRec(rest, false, result += curChar ++= blockBody)
          } else {  // not start of an expression
            escapeRec(sTail, false, result ++= escapeChar(curChar))
          }
        }
      }

    }

    escapeRec(s)
  }

}

object Escapable extends Escapable
