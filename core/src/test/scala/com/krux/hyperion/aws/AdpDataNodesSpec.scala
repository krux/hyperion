package com.krux.hyperion.aws

import org.json4s.JsonDSL._
import org.scalatest.WordSpec

class AdpDataNodesSpec extends WordSpec {

  val tsvFormat = AdpTsvDataFormat("tsv", None, None, None)

  "AdpS3DirectoryDataNode" should {
    "converts to Json" in {
      val testObj = AdpS3DataNode(
        id = "s3dn",
        name = None,
        directoryPath = Option("s3://blah/blah"),
        filePath = None,
        dataFormat = Option(AdpRef(tsvFormat)),
        manifestFilePath = None,
        compression = Option("gzip"),
        s3EncryptionType = None,
        precondition = None,
        onSuccess = None,
        onFail = None
      )
      val objShouldBe = ("id" -> "s3dn") ~
        ("compression" -> "gzip") ~
        ("dataFormat" -> ("ref" -> "tsv")) ~
        ("directoryPath" -> "s3://blah/blah") ~
        ("type" -> "S3DataNode")
      assert(AdpJsonSerializer(testObj) === objShouldBe)
    }
  }

  "AdpS3FileDataNode" should {
    "converts to Json" in {
      val testObj = AdpS3DataNode(
        id = "s3dn",
        name = None,
        directoryPath = None,
        filePath = Option("s3://blah/blah/dir/"),
        dataFormat = Option(AdpRef(tsvFormat)),
        manifestFilePath = None,
        compression = Option("gzip"),
        s3EncryptionType = None,
        precondition = None,
        onSuccess = None,
        onFail = None
      )
      val objShouldBe = ("id" -> "s3dn") ~
        ("compression" -> "gzip") ~
        ("dataFormat" -> ("ref" -> "tsv")) ~
        ("filePath" -> "s3://blah/blah/dir/") ~
        ("type" -> "S3DataNode")

      assert(AdpJsonSerializer(testObj) === objShouldBe)
    }
  }

  "AdpRedshiftDataNode" should {
    "converts to Json" in {
      val testObj = AdpRedshiftDataNode(
        id = "redshiftTable",
        name = None,
        createTableSql = None,
        database = AdpRef.withRefObjId[AdpRedshiftDatabase]("myRedshift"),
        schemaName = Option("public"),
        tableName = "myTable",
        primaryKeys = None,
        precondition = None,
        onSuccess = None,
        onFail = None
      )
      val objShouldBe = ("id" -> "redshiftTable") ~
        ("database" -> ("ref" -> "myRedshift")) ~
        ("schemaName" -> "public") ~
        ("tableName" -> "myTable") ~
        ("type" -> "RedshiftDataNode")

      assert(AdpJsonSerializer(testObj) === objShouldBe)
    }
  }
}
