package com.krux.hyperion.datanode

trait TableQuery {
  def table: String
  def columns: List[String]
  def columnsString = columns.mkString(", ")
  def sql: String
}

case class SelectTableQuery(table: String, columns: List[String], whereClause: Option[String]) extends TableQuery {

  def sql = s"select $columnsString from $table ${whereClause.map("where " + _).getOrElse("")}"

}

/**
 * @note this is not tested or documented anywhere in AWS Data Pipeline Docs, we are assuming the
 * insert syntax is 'insert into mytable (col1, col2, ...)' without 'values'.
 */
case class InsertTableQuery(table: String, columns: List[String]) extends TableQuery {

  def sql = s"insert into $table ($columnsString)"

}
