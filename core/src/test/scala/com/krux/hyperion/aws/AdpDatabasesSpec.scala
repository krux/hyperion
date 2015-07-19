package com.krux.hyperion.aws

import org.scalatest.WordSpec
import org.json4s.JsonDSL._

class AdpDatabasesSpec extends WordSpec {

  "AdpRedshiftDatabase" should {
    "converts to Json" in {
      val testObj = AdpRedshiftDatabase(
        "red",
        None,
        "datascience-redshift-id",
        None,
        Some("dbname"),
        None,
        "supersecretpassword",
        "notsosupersecretuser"
      )
      val objShoudBe = ("id" -> "red") ~
        ("clusterId" -> "datascience-redshift-id") ~
        ("databaseName" -> "dbname") ~
        ("*password" -> "supersecretpassword") ~
        ("username" -> "notsosupersecretuser") ~
        ("type" -> "RedshiftDatabase")

      assert(AdpJsonSerializer(testObj) === objShoudBe)
    }
  }

  "AdpJdbcDatabase" should {
    "converts to Json" in {
      val testObj = AdpJdbcDatabase(
        id = "jdbc",
        name = None,
        connectionString = "datascience-mysql",
        jdbcDriverClass = "mysql",
        databaseName = Some("dbname"),
        jdbcProperties = None,
        `*password` = "supersecretpassword",
        username = "notsosupersecretuser"
      )
      val objShoudBe = ("id" -> "jdbc") ~
        ("connectionString" -> "datascience-mysql") ~
        ("jdbcDriverClass" -> "mysql") ~
        ("databaseName" -> "dbname") ~
        ("*password" -> "supersecretpassword") ~
        ("username" -> "notsosupersecretuser") ~
        ("type" -> "JdbcDatabase")

      assert(AdpJsonSerializer(testObj) === objShoudBe)
    }
  }

  "AdpRdsDatabase" should {
    "converts to Json" in {
      val testObj = AdpRdsDatabase(
        id = "rds",
        name = None,
        databaseName = Some("datascience-rds"),
        jdbcProperties = None,
        `*password` = "supersecretpassword",
        username = "notsosupersecretuser"
      )
      val objShoudBe = ("id" -> "rds") ~
        ("databaseName" -> "datascience-rds") ~
        ("*password" -> "supersecretpassword") ~
        ("username" -> "notsosupersecretuser") ~
        ("type" -> "RdsDatabase")

      assert(AdpJsonSerializer(testObj) === objShoudBe)
    }
  }

}
