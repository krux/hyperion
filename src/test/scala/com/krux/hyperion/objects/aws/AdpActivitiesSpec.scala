package com.krux.hyperion.objects.aws

import org.scalatest.WordSpec
import org.json4s.JsonDSL._

class AdpActivitiesSpec extends WordSpec {

  "AdpRedshiftCopyActivity" should {
    "converts to json" in {
      val testObj = AdpRedshiftCopyActivity(
        id = "S3ToRedshiftCopyActivity",
        name = None,
        input = AdpRef[AdpDataNode]("MyS3DataNode"),
        insertMode = "KEEP_EXISTING",
        output = AdpRef[AdpDataNode]("MyRedshiftDataNode"),
        runsOn = AdpRef[AdpEc2Resource]("MyEc2Resource"),
        transformSql = None,
        commandOptions = Some(Seq("EMPTYASNULL", "IGNOREBLANKLINES")),
        queue = None,
        dependsOn = None,
        onFail = None,
        onSuccess = None,
        onLateAction = None
      )
      val objShouldBe = ("id" -> "S3ToRedshiftCopyActivity") ~
        ("input" -> ("ref" -> "MyS3DataNode")) ~
        ("insertMode" -> "KEEP_EXISTING") ~
        ("output" -> ("ref" -> "MyRedshiftDataNode")) ~
        ("runsOn" -> ("ref" -> "MyEc2Resource")) ~
        ("commandOptions" -> Seq("EMPTYASNULL", "IGNOREBLANKLINES")) ~
        ("type" -> "RedshiftCopyActivity")
      assert(AdpJsonSerializer(testObj) === objShouldBe)
    }
  }

  "AdpEmrActivity" should {
    "converts to json" in {
      val testObj = AdpEmrActivity(
          id = "MyEmrActivity",
          name = None,
          runsOn = AdpRef[AdpEmrCluster]("MyEmrCluster"),
          preStepCommand = Some(Seq("scp remoteFiles localFiles")),
          step = Seq(
            "s3://myBucket/myPath/myStep.jar,firstArg,secondArg",
            "s3://myBucket/myPath/myOtherStep.jar,anotherArg"
          ),
          postStepCommand = Some(Seq("scp localFiles remoteFiles")),
          input = Some(AdpRef[AdpS3DataNode]("MyS3Input")),
          output = Some(AdpRef[AdpS3DataNode]("MyS3Output")),
          dependsOn = None,
          onFail = None,
          onSuccess = None,
          onLateAction = None
        )
      val objShouldBe = ("id" -> "MyEmrActivity") ~
        ("input" ->  ("ref" -> "MyS3Input")) ~
        ("output" -> ("ref" -> "MyS3Output")) ~
        ("preStepCommand" -> Seq("scp remoteFiles localFiles")) ~
        ("postStepCommand" -> Seq("scp localFiles remoteFiles")) ~
        ("runsOn" -> ("ref" -> "MyEmrCluster")) ~
        ("step" -> Seq(
          "s3://myBucket/myPath/myStep.jar,firstArg,secondArg",
          "s3://myBucket/myPath/myOtherStep.jar,anotherArg")) ~
        ("type" -> "EmrActivity")
      assert(AdpJsonSerializer(testObj) === objShouldBe)
    }
  }

}
