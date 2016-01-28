package com.krux.hyperion.aws

import org.json4s.JsonDSL._
import org.scalatest.WordSpec

class AdpDatabasesSpec extends WordSpec {

  "AdpRedshiftDatabase" should {
    "converts to Json" in {
      val testObj = AdpRedshiftDatabase(
        id = "red",
        name = None,
        clusterId = "datascience-redshift-id",
        connectionString = None,
        databaseName = Option("dbname"),
        `*password` = "supersecretpassword",
        username = "notsosupersecretuser",
        jdbcProperties = None
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
        databaseName = Option("dbname"),
        username = "notsosupersecretuser",
        `*password` = "supersecretpassword",
        jdbcDriverJarUri = None,
        jdbcDriverClass = "mysql",
        jdbcProperties = None
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
        databaseName = Option("datascience-rds"),
        jdbcProperties = None,
        `*password` = "supersecretpassword",
        username = "notsosupersecretuser",
        rdsInstanceId = "21234",
        region = Option("us-west-2"),
        jdbcDriverJarUri = None
      )
      val objShoudBe = ("id" -> "rds") ~
        ("databaseName" -> "datascience-rds") ~
        ("*password" -> "supersecretpassword") ~
        ("username" -> "notsosupersecretuser") ~
        ("type" -> "RdsDatabase") ~
        ("rdsInstanceId" -> "21234") ~
        ("region" -> "us-west-2")

      assert(AdpJsonSerializer(testObj) === objShoudBe)
    }
  }

}
