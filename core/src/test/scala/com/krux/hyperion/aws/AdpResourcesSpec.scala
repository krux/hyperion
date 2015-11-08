package com.krux.hyperion.aws

import org.scalatest.WordSpec
import org.json4s.JsonDSL._

class AdpResourcesSpec extends WordSpec {

  "AdpEc2Resource" should {
    "converts to Json" in {

      val testObj = AdpEc2Resource(
        id = "theId",
        name = None,
        instanceType = None,
        imageId = None,
        role = None,
        resourceRole = None,
        runAsUser = None,
        keyPair = None,
        region = None,
        availabilityZone = None,
        subnetId = None,
        associatePublicIpAddress = None,
        securityGroups = Option(Seq("krux-periodic")),
        securityGroupIds = None,
        spotBidPrice = None,
        useOnDemandOnLastAttempt = None,
        initTimeout = None,
        terminateAfter = Option("4 hours"),
        actionOnResourceFailure = None,
        actionOnTaskFailure = None,
        httpProxy = None
      )

      val objShouldBe = ("id" -> "theId") ~
        ("terminateAfter" -> "4 hours") ~
        ("securityGroups" -> Seq("krux-periodic")) ~
        ("type" -> "Ec2Resource")

      assert(AdpJsonSerializer(testObj) === objShouldBe)
    }
  }

  "AdpEmrCluster" should {
    "converts to json" in {
      val testObj = new AdpEmrCluster(
        id = "theId",
        name = None,
        amiVersion = Option("3.3"),
        supportedProducts = None,
        bootstrapAction = Seq("s3://blah1", "s3://blah2"),
        enableDebugging = None,
        hadoopSchedulerType = None,
        keyPair = None,
        masterInstanceBidPrice = None,
        masterInstanceType = Option("m3.xlarge"),
        coreInstanceBidPrice = None,
        coreInstanceCount = Option("2"),
        coreInstanceType = Option("m3.xlarge"),
        taskInstanceBidPrice = None,
        taskInstanceCount = Option("4"),
        taskInstanceType = Option("m3.xlarge"),
        region = None,
        availabilityZone = None,
        resourceRole = None,
        role = None,
        subnetId = None,
        masterSecurityGroupId = None,
        additionalMasterSecurityGroupIds = None,
        slaveSecurityGroupId = None,
        additionalSlaveSecurityGroupIds = None,
        useOnDemandOnLastAttempt = None,
        visibleToAllUsers = None,
        initTimeout = None,
        terminateAfter = Option("8 hours"),
        actionOnResourceFailure = None,
        actionOnTaskFailure = None,
        httpProxy = None
      )

      val objShouldBe = ("id" -> "theId") ~
        ("bootstrapAction" -> Seq("s3://blah1", "s3://blah2")) ~
        ("amiVersion" -> "3.3") ~
        ("masterInstanceType" -> "m3.xlarge") ~
        ("coreInstanceType" -> "m3.xlarge") ~
        ("coreInstanceCount" -> "2") ~
        ("taskInstanceType" -> "m3.xlarge") ~
        ("taskInstanceCount" -> "4") ~
        ("terminateAfter" -> "8 hours") ~
        ("type" -> "EmrCluster")

      assert(AdpJsonSerializer(testObj) === objShouldBe)
    }
  }
}
