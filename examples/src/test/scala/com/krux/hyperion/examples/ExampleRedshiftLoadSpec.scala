package com.krux.hyperion.examples

import org.json4s._
import org.json4s.JsonDSL._
import org.scalatest.WordSpec


class ExampleRedshiftLoadSpec extends WordSpec {

  "ExampleRedshiftLoad" should {

    "produce correct pipeline JSON" in {

      val pipelineJson = ExampleRedshiftLoad.toJson
      val objectsField = pipelineJson.children(0).children.sortBy(o => (o \ "name").toString)

      // have the correct number of objects
      assert(objectsField.size === 8)

      val default = objectsField(0)
      val defaultShouldBe =
        ("id" -> "Default") ~
        ("name" -> "Default") ~
        ("scheduleType" -> "cron") ~
        ("failureAndRerunMode" -> "CASCADE") ~
        ("pipelineLogUri" -> "s3://your-bucket/datapipeline-logs/") ~
        ("role" -> "DataPipelineDefaultRole") ~
        ("resourceRole" -> "DataPipelineDefaultResourceRole") ~
        ("schedule" -> ("ref" -> "PipelineSchedule"))
      assert(default === defaultShouldBe)

      val ec2 = objectsField(1)
      val ec2Id: String = (ec2 \ "id").values.toString
      assert(ec2Id.startsWith("Ec2Resource"))
      val ec2ShouldBe =
        ("id" -> ec2Id) ~
        ("name" -> ec2Id) ~
        ("terminateAfter" -> "8 hours") ~
        ("imageId" -> "ami-f6795a8c") ~
        ("instanceType" -> "m1.small") ~
        ("region" -> "us-east-1") ~
        ("securityGroups" -> Seq("your-security-group")) ~
        ("associatePublicIpAddress" -> "false") ~
        ("keyPair" -> "your-aws-key-pair") ~
        ("type" -> "Ec2Resource") ~
        ("role" -> "DataPipelineDefaultRole") ~
        ("resourceRole" -> "DataPipelineDefaultResourceRole") ~
        ("initTimeout" -> "1 hours")
      assert(ec2 === ec2ShouldBe)

      val pipelineSchedule = objectsField(2)
      val pipelineScheduleShouldBe =
        ("id" -> "PipelineSchedule") ~
        ("name" -> "PipelineSchedule") ~
        ("period" -> "1 hours") ~
        ("startAt" -> "FIRST_ACTIVATION_DATE_TIME") ~
        ("type" -> "Schedule")
      assert(pipelineSchedule === pipelineScheduleShouldBe)

      val tsv = objectsField(6)
      val tsvId: String = (tsv \ "id").values.toString
      assert(tsvId.startsWith("TsvDataFormat_"))
      val tsvShouldBe =
        ("id" -> tsvId) ~
        ("name" -> tsvId) ~
        ("type" -> "TSV")
      assert(tsv === tsvShouldBe)

      val s3DataNode = objectsField(5)
      val s3DataNodeId: String = (s3DataNode \ "id").values.toString
      assert(s3DataNodeId.startsWith("S3Folder_"))
      val s3DataNodeShouldBe =
        ("id" -> s3DataNodeId) ~
        ("name" -> s3DataNodeId) ~
        ("dataFormat" -> ("ref" -> tsvId)) ~
        ("directoryPath" -> "s3://testing/testtab/") ~
        ("type" -> "S3DataNode")
      assert(s3DataNode === s3DataNodeShouldBe)

      val mockRedshift = objectsField(7)
      val mockRedshiftId: String = (mockRedshift \ "id").values.toString
      assert(mockRedshiftId.startsWith("RdsDatabase_"))
      val mockRedshiftShouldBe =
        ("id" -> mockRedshiftId) ~
        ("name" -> "_MockRedshift") ~
        ("clusterId" -> "mock-redshift") ~
        ("databaseName" -> "mock_db") ~
        ("*password" -> "mockpass") ~
        ("username" -> "mockuser") ~
        ("type" -> "RedshiftDatabase")
      assert(mockRedshift === mockRedshiftShouldBe)

      val destTable = objectsField(4)
      val destTableId = (destTable \ "id").values.toString
      assert(destTableId.startsWith("RedshiftDataNode_"))
      val destTableShouldBe =
        ("id" -> destTableId) ~
        ("name" -> destTableId) ~
        ("database" -> ("ref" -> mockRedshiftId)) ~
        ("schemaName" -> "kexin") ~
        ("tableName" -> "monthly_campaign_frequency_distribution") ~
        ("primaryKeys" -> List("publisher_id", "campaign_id", "month")) ~
        ("type" -> "RedshiftDataNode")
      assert(destTable === destTableShouldBe)

      val copy = objectsField(3)
      val copyNodeId: String = (copy \ "id").values.toString
      assert(copyNodeId.startsWith("RedshiftCopyActivity_"))
      val copyShouldBe =
        ("id" -> copyNodeId) ~
        ("name" -> copyNodeId) ~
        ("input" -> ("ref" -> s3DataNodeId)) ~
        ("insertMode" -> "OVERWRITE_EXISTING") ~
        ("output" -> ("ref" -> destTableId)) ~
        ("runsOn" -> ("ref" -> ec2Id)) ~
        ("type" -> "RedshiftCopyActivity")
      assert(copy === copyShouldBe)

    }
  }

}
