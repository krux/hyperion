package com.krux.hyperion.activity

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.resource.Ec2Resource
import com.typesafe.config.ConfigFactory
import org.scalatest.WordSpec

class JarActivitySpec extends WordSpec {
  class SomeClass

  object SomeObject {

  }

  implicit val hc: HyperionContext = new HyperionContext(ConfigFactory.load("example"))
  val ec2 = Ec2Resource()

  "JarActivitySpec" should {
    "allow mainClass from a String" in {
      val name = "com.foo.MainClass"
      val ja = JarActivity("something.jar", ec2).withMainClass(name)
      assert(ja.mainClass == Some(name))
    }

    "allow mainClass an instance" in {
      val ja = JarActivity("something.jar", ec2).withMainClass(new SomeClass())
      assert(ja.mainClass == Some("com.krux.hyperion.activity.JarActivitySpec$SomeClass"))
    }

    "allow mainClass an object" in {
      val ja = JarActivity("something.jar", ec2).withMainClass(SomeObject)
      assert(ja.mainClass == Some("com.krux.hyperion.activity.JarActivitySpec$SomeObject"))
    }

    "allow mainClass a Class" in {
      val ja = JarActivity("something.jar", ec2).withMainClass(SomeObject.getClass)
      assert(ja.mainClass == Some("com.krux.hyperion.activity.JarActivitySpec$SomeObject"))
    }
  }
}
