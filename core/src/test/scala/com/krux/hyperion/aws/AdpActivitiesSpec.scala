package com.krux.hyperion.aws

import org.json4s.JsonDSL._
import org.scalatest.WordSpec

class AdpActivitiesSpec extends WordSpec {
  "AdpCopyActivity" should {
    "converts to json" in {
      val testObj = AdpCopyActivity(
        id = "GenericCopyActivity",
        name = None,
        input = AdpRef.withRefObjId[AdpDataNode]("MyS3DataNode"),
        output = AdpRef.withRefObjId[AdpDataNode]("MyOtherS3DataNode"),
        workerGroup = None,
        runsOn = Option(AdpRef.withRefObjId[AdpEc2Resource]("MyEc2Resource")),
        dependsOn = None,
        precondition = None,
        onFail = None,
        onSuccess = None,
        onLateAction = None,
        attemptTimeout = None,
        lateAfterTimeout = None,
        maximumRetries = None,
        retryDelay = None,
        failureAndRerunMode = None,
        maxActiveInstances = None
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
        insertMode = "KEEP_EXISTING",
        transformSql = None,
        queue = None,
        commandOptions = Option(Seq("EMPTYASNULL", "IGNOREBLANKLINES")),
        input = AdpRef.withRefObjId[AdpDataNode]("MyS3DataNode"),
        output = AdpRef.withRefObjId[AdpDataNode]("MyRedshiftDataNode"),
        workerGroup = None,
        runsOn = Option(AdpRef.withRefObjId[AdpEc2Resource]("MyEc2Resource")),
        dependsOn = None,
        precondition = None,
        onFail = None,
        onSuccess = None,
        onLateAction = None,
        attemptTimeout = None,
        lateAfterTimeout = None,
        maximumRetries = None,
        retryDelay = None,
        failureAndRerunMode = None,
        maxActiveInstances = None
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
        step = Seq(
          "s3://myBucket/myPath/myStep.jar,firstArg,secondArg",
          "s3://myBucket/myPath/myOtherStep.jar,anotherArg"
        ),
        preStepCommand = Option(Seq("scp remoteFiles localFiles")),
        postStepCommand = Option(Seq("scp localFiles remoteFiles")),
        input = Option(Seq(AdpRef.withRefObjId[AdpS3DataNode]("MyS3Input"))),
        output = Option(Seq(AdpRef.withRefObjId[AdpS3DataNode]("MyS3Output"))),
        workerGroup = None,
        runsOn = Option(AdpRef.withRefObjId[AdpEmrCluster]("MyEmrCluster")),
        dependsOn = None,
        precondition = None,
        onFail = None,
        onSuccess = None,
        onLateAction = None,
        attemptTimeout = None,
        lateAfterTimeout = None,
        maximumRetries = None,
        retryDelay = None,
        failureAndRerunMode = None,
        maxActiveInstances = None
      )
      val objShouldBe = ("id" -> "MyEmrActivity") ~
        ("input" ->  Seq(("ref" -> "MyS3Input"))) ~
        ("output" -> Seq(("ref" -> "MyS3Output"))) ~
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
      val testObj = new AdpHiveActivity(
        id = "HiveActivity",
        name = None,
        hiveScript = Option("SELECT * FROM TABLE"),
        scriptUri = None,
        scriptVariable = None,
        hadoopQueue = None,
        stage = Option("true"),
        input = Option(AdpRef.withRefObjId[AdpDataNode]("MyS3DataNode")),
        output = Option(AdpRef.withRefObjId[AdpDataNode]("MyOtherS3DataNode")),
        preActivityTaskConfig = None,
        postActivityTaskConfig = None,
        workerGroup = None,
        runsOn = Option(AdpRef.withRefObjId[AdpEmrCluster]("MyEc2Resource")),
        dependsOn = None,
        precondition = None,
        onFail = None,
        onSuccess = None,
        onLateAction = None,
        attemptTimeout = None,
        lateAfterTimeout = None,
        maximumRetries = None,
        retryDelay = None,
        failureAndRerunMode = None,
        maxActiveInstances = None
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
        filterSql = Option("SELECT * FROM TABLE"),
        generatedScriptsPath = None,
        input = Option(AdpRef.withRefObjId[AdpDataNode]("MyS3DataNode")),
        output = Option(AdpRef.withRefObjId[AdpDataNode]("MyOtherS3DataNode")),
        preActivityTaskConfig = None,
        postActivityTaskConfig = None,
        workerGroup = None,
        runsOn = Option(AdpRef.withRefObjId[AdpEmrCluster]("MyEmrResource")),
        dependsOn = None,
        precondition = None,
        onFail = None,
        onSuccess = None,
        onLateAction = None,
        attemptTimeout = None,
        lateAfterTimeout = None,
        maximumRetries = None,
        retryDelay = None,
        failureAndRerunMode = None,
        maxActiveInstances = None
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
      val testObj = new AdpPigActivity(
        id = "PigActivity",
        name = None,
        script = Option("SELECT * FROM TABLE"),
        scriptUri = None,
        scriptVariable = None,
        generatedScriptsPath = None,
        stage = Option("true"),
        input = Option(AdpRef.withRefObjId[AdpDataNode]("MyS3DataNode")),
        output = Option(AdpRef.withRefObjId[AdpDataNode]("MyOtherS3DataNode")),
        preActivityTaskConfig = None,
        postActivityTaskConfig = None,
        workerGroup = None,
        runsOn = Option(AdpRef.withRefObjId[AdpEmrCluster]("MyEmrResource")),
        dependsOn = None,
        precondition = None,
        onFail = None,
        onSuccess = None,
        onLateAction = None,
        attemptTimeout = None,
        lateAfterTimeout = None,
        maximumRetries = None,
        retryDelay = None,
        failureAndRerunMode = None,
        maxActiveInstances = None
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
        script = Option("Script"),
        scriptUri = None,
        scriptArgument = None,
        database = AdpRef.withRefObjId[AdpDatabase]("MyDatabase"),
        queue = Option("yes"),
        workerGroup = None,
        runsOn = Option(AdpRef.withRefObjId[AdpEc2Resource]("MyEc2Resource")),
        dependsOn = None,
        precondition = None,
        onFail = None,
        onSuccess = None,
        onLateAction = None,
        attemptTimeout = None,
        lateAfterTimeout = None,
        maximumRetries = None,
        retryDelay = None,
        failureAndRerunMode = None,
        maxActiveInstances = None
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
      val testObj = new AdpShellCommandActivity(
        id = "ShellCommandActivity",
        name = None,
        command = Option("rm -rf /"),
        scriptUri = None,
        scriptArgument = None,
        stdout = Option("log.out"),
        stderr = Option("log.err"),
        stage = Option("true"),
        input = Option(Seq(AdpRef.withRefObjId[AdpDataNode]("MyS3DataNode"))),
        output = Option(Seq(AdpRef.withRefObjId[AdpDataNode]("MyOtherS3DataNode"))),
        workerGroup = None,
        runsOn = Option(AdpRef.withRefObjId[AdpEc2Resource]("MyEc2Resource")),
        dependsOn = None,
        precondition = None,
        onFail = None,
        onSuccess = None,
        onLateAction = None,
        attemptTimeout = None,
        lateAfterTimeout = None,
        maximumRetries = None,
        retryDelay = None,
        failureAndRerunMode = None,
        maxActiveInstances = None
      )
      val objShouldBe = ("id" -> "ShellCommandActivity") ~
        ("command" -> "rm -rf /") ~
        ("input" -> Seq("ref" -> "MyS3DataNode")) ~
        ("output" -> Seq("ref" -> "MyOtherS3DataNode")) ~
        ("stage" -> "true") ~
        ("stdout" -> "log.out") ~
        ("stderr" -> "log.err") ~
        ("runsOn" -> ("ref" -> "MyEc2Resource")) ~
        ("type" -> "ShellCommandActivity")
      assert(AdpJsonSerializer(testObj) === objShouldBe)
    }
  }

}
