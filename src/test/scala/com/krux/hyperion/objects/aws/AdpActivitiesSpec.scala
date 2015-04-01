package com.krux.hyperion.objects.aws

import org.scalatest.WordSpec
import org.json4s.JsonDSL._

class AdpActivitiesSpec extends WordSpec {
  "AdpCopyActivity" should {
    "converts to json" in {
      val testObj = AdpCopyActivity(
        id = "GenericCopyActivity",
        name = None,
        input = AdpRef[AdpDataNode]("MyS3DataNode"),
        output = AdpRef[AdpDataNode]("MyOtherS3DataNode"),
        runsOn = AdpRef[AdpEc2Resource]("MyEc2Resource"),
        dependsOn = None,
        precondition = None,
        onFail = None,
        onSuccess = None,
        onLateAction = None
      )
      val objShouldBe = ("id" -> "GenericCopyActivity") ~
        ("input" -> ("ref" -> "MyS3DataNode")) ~
        ("output" -> ("ref" -> "MyOtherS3DataNode")) ~
        ("runsOn" -> ("ref" -> "MyEc2Resource")) ~
        ("type" -> "CopyActivity")
      assert(AdpJsonSerializer(testObj) === objShouldBe)
    }
  }

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
        precondition = None,
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
        postStepCommand = Some(Seq("scp localFiles remoteFiles")),
        actionOnResourceFailure = None,
        actionOnTaskFailure = None,
        step = Seq(
          "s3://myBucket/myPath/myStep.jar,firstArg,secondArg",
          "s3://myBucket/myPath/myOtherStep.jar,anotherArg"
        ),
        input = Some(AdpRef[AdpS3DataNode]("MyS3Input")),
        output = Some(AdpRef[AdpS3DataNode]("MyS3Output")),
        dependsOn = None,
        precondition = None,
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

  "AdpHiveActivity" should {
    "converts to json" in {
      val testObj = AdpHiveActivity(
        id = "HiveActivity",
        name = None,
        hiveScript = Some("SELECT * FROM TABLE"),
        scriptUri = None,
        scriptVariable = None,
        input = AdpRef[AdpDataNode]("MyS3DataNode"),
        output = AdpRef[AdpDataNode]("MyOtherS3DataNode"),
        stage = "true",
        runsOn = AdpRef[AdpEmrCluster]("MyEc2Resource"),
        dependsOn = None,
        precondition = None,
        onFail = None,
        onSuccess = None,
        onLateAction = None
      )
      val objShouldBe = ("id" -> "HiveActivity") ~
        ("hiveScript" -> "SELECT * FROM TABLE") ~
        ("input" -> ("ref" -> "MyS3DataNode")) ~
        ("output" -> ("ref" -> "MyOtherS3DataNode")) ~
        ("stage" -> "true") ~
        ("runsOn" -> ("ref" -> "MyEc2Resource")) ~
        ("type" -> "HiveActivity")
      assert(AdpJsonSerializer(testObj) === objShouldBe)
    }
  }

  "AdpHiveCopyActivity" should {
    "converts to json" in {
      val testObj = AdpHiveCopyActivity(
        id = "HiveCopyActivity",
        name = None,
        filterSql = Some("SELECT * FROM TABLE"),
        generatedScriptsPath = None,
        input = AdpRef[AdpDataNode]("MyS3DataNode"),
        output = AdpRef[AdpDataNode]("MyOtherS3DataNode"),
        runsOn = AdpRef[AdpEmrCluster]("MyEmrResource"),
        dependsOn = None,
        precondition = None,
        onFail = None,
        onSuccess = None,
        onLateAction = None
      )
      val objShouldBe = ("id" -> "HiveCopyActivity") ~
        ("filterSql" -> "SELECT * FROM TABLE") ~
        ("input" -> ("ref" -> "MyS3DataNode")) ~
        ("output" -> ("ref" -> "MyOtherS3DataNode")) ~
        ("runsOn" -> ("ref" -> "MyEmrResource")) ~
        ("type" -> "HiveCopyActivity")
      assert(AdpJsonSerializer(testObj) === objShouldBe)
    }
  }

  "AdpPigActivity" should {
    "converts to json" in {
      val testObj = AdpPigActivity(
        id = "PigActivity",
        name = None,
        generatedScriptsPath = None,
        script = Some("SELECT * FROM TABLE"),
        scriptUri = None,
        scriptVariable = None,
        input = AdpRef[AdpDataNode]("MyS3DataNode"),
        output = AdpRef[AdpDataNode]("MyOtherS3DataNode"),
        stage = "true",
        runsOn = AdpRef[AdpEmrCluster]("MyEmrResource"),
        dependsOn = None,
        precondition = None,
        onFail = None,
        onSuccess = None,
        onLateAction = None
      )
      val objShouldBe = ("id" -> "PigActivity") ~
        ("script" -> "SELECT * FROM TABLE") ~
        ("input" -> ("ref" -> "MyS3DataNode")) ~
        ("output" -> ("ref" -> "MyOtherS3DataNode")) ~
        ("stage" -> "true") ~
        ("runsOn" -> ("ref" -> "MyEmrResource")) ~
        ("type" -> "PigActivity")
      assert(AdpJsonSerializer(testObj) === objShouldBe)
    }
  }

  "AdpSqlActivity" should {
    "converts to json" in {
      val testObj = AdpSqlActivity(
        id = "SqlActivity",
        name = None,
        database = AdpRef[AdpDatabase]("MyDatabase"),
        script = "Script",
        scriptArgument = None,
        queue = Some("yes"),
        runsOn = AdpRef[AdpEc2Resource]("MyEc2Resource"),
        dependsOn = None,
        precondition = None,
        onFail = None,
        onSuccess = None,
        onLateAction = None
      )
      val objShouldBe = ("id" -> "SqlActivity") ~
        ("database" -> ("ref" -> "MyDatabase")) ~
        ("script" -> "Script") ~
        ("queue" -> "yes") ~
        ("runsOn" -> ("ref" -> "MyEc2Resource")) ~
        ("type" -> "SqlActivity")
      assert(AdpJsonSerializer(testObj) === objShouldBe)
    }
  }

  "AdpShellCommandActivity" should {
    "converts to json" in {
      val testObj = AdpShellCommandActivity(
        id = "ShellCommandActivity",
        name = None,
        command = Some("rm -rf /"),
        scriptUri = None,
        scriptArgument = None,
        input = Some(Seq(AdpRef[AdpDataNode]("MyS3DataNode"))),
        output = Some(Seq(AdpRef[AdpDataNode]("MyOtherS3DataNode"))),
        stage = "true",
        stdout = Some("log.out"),
        stderr = Some("log.err"),
        runsOn = AdpRef[AdpEc2Resource]("MyEc2Resource"),
        dependsOn = None,
        precondition = None,
        onFail = None,
        onSuccess = None,
        onLateAction = None
      )
      val objShouldBe = ("id" -> "ShellCommandActivity") ~
        ("command" -> "rm -rf /") ~
        ("input" -> Seq(("ref" -> "MyS3DataNode"))) ~
        ("output" -> Seq(("ref" -> "MyOtherS3DataNode"))) ~
        ("stage" -> "true") ~
        ("stdout" -> "log.out") ~
        ("stderr" -> "log.err") ~
        ("runsOn" -> ("ref" -> "MyEc2Resource")) ~
        ("type" -> "ShellCommandActivity")
      assert(AdpJsonSerializer(testObj) === objShouldBe)
    }
  }

}
