package com.krux.hyperion.activity

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.common.S3Uri
import com.krux.hyperion.resource.Ec2Resource
import com.typesafe.config.ConfigFactory
import org.scalatest.WordSpec

class MapReduceStepSpec extends WordSpec {
  class SomeClass

  object SomeObject {

  }

  implicit val hc: HyperionContext = new HyperionContext(ConfigFactory.load("example"))
  val ec2 = Ec2Resource()

  "MapReduceSpec" should {
    "allow mainClass from a String" in {
      val name = "com.foo.SomeClass"
      val ja = JarActivity(S3Uri("s3://something.jar"))(ec2).withMainClass(name)
      assert(ja.mainClass.map(_.toString) == Some(name))
    }

    "allow mainClass an instance" in {
      val ja = JarActivity(S3Uri("s3://something.jar"))(ec2).withMainClass(new SomeClass())
      assert(ja.mainClass.map(_.toString) == Some("com.krux.hyperion.activity.MapReduceStepSpec.SomeClass"))
    }

    "allow mainClass an object" in {
      val ja = JarActivity(S3Uri("s3://something.jar"))(ec2).withMainClass(SomeObject)
      assert(ja.mainClass.map(_.toString) == Some("com.krux.hyperion.activity.MapReduceStepSpec.SomeObject"))
    }

    "allow mainClass a Class" in {
      val ja = JarActivity(S3Uri("s3://something.jar"))(ec2).withMainClass(SomeObject.getClass)
      assert(ja.mainClass.map(_.toString) == Some("com.krux.hyperion.activity.MapReduceStepSpec.SomeObject"))
    }
  }
}
