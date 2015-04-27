package com.krux.hyperion.objects.aws

import org.scalatest.WordSpec
import org.json4s.JsonDSL._

class AdpEc2ResourcesSpec extends WordSpec {

  "AdpEc2Resource" should {
    "converts to Json" in {

      val testObj = AdpEc2Resource(
        id = "theId",
        name = None,
        terminateAfter = "4 hours",
        role = None,
        resourceRole = None,
        imageId = None,
        instanceType = None,
        region = None,
        securityGroups = Some(Seq("krux-periodic")),
        securityGroupIds = None,
        associatePublicIpAddress = None,
        keyPair = None,
        subnetId = None
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
      val testObj = AdpEmrCluster(
        id = "theId",
        name = None,
        bootstrapAction = Seq("s3://blah1", "s3://blah2"),
        amiVersion = Some("3.3"),
        masterInstanceType = Some("m3.xlarge"),
        coreInstanceType = Some("m3.xlarge"),
        coreInstanceCount = Some("2"),
        taskInstanceType = Some("m3.xlarge"),
        taskInstanceCount = Some("4"),
        taskInstanceBidPrice = None,
        terminateAfter = "8 hours",
        keyPair = None,
        region = None,
        enableDebugging = None,
        supportedProducts = None,
        subnetId = None
      )

      val objShoudBe = ("id" -> "theId") ~
        ("bootstrapAction" -> Seq("s3://blah1", "s3://blah2")) ~
        ("amiVersion" -> "3.3") ~
        ("masterInstanceType" -> "m3.xlarge") ~
        ("coreInstanceType" -> "m3.xlarge") ~
        ("coreInstanceCount" -> "2") ~
        ("taskInstanceType" -> "m3.xlarge") ~
        ("taskInstanceCount" -> "4") ~
        ("terminateAfter" -> "8 hours") ~
        ("type" -> "EmrCluster")

      assert(AdpJsonSerializer(testObj) === objShoudBe)
    }
  }
}
