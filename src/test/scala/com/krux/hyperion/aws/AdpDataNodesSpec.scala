package com.krux.hyperion.aws

import org.scalatest.WordSpec
import org.json4s.JsonDSL._

class AdpDataNodesSpec extends WordSpec {

  val tsvFormat = AdpTsvDataFormat("tsv", None, None, None)

  "AdpS3DirectoryDataNode" should {
    "converts to Json" in {
      val testObj = AdpS3DirectoryDataNode(
        id = "s3dn",
        name = None,
        compression = Some("gzip"),
        dataFormat = Some(AdpRef(tsvFormat)),
        directoryPath = "s3://blah/blah",
        manifestFilePath = None,
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
      val testObj = AdpS3FileDataNode(
        id = "s3dn",
        name = None,
        compression = Some("gzip"),
        dataFormat = Some(AdpRef(tsvFormat)),
        filePath = "s3://blah/blah/dir/",
        manifestFilePath = None,
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
        schemaName = Some("public"),
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
