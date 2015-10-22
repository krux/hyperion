package com.krux.hyperion.activity

import com.typesafe.config.ConfigFactory
import org.scalatest.WordSpec

import com.krux.hyperion.common.{PipelineObjectId, S3Uri}
import com.krux.hyperion.common.S3Uri._
import com.krux.hyperion.database.RedshiftDatabase
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.Implicits._
import com.krux.hyperion.resource.Ec2Resource
import com.krux.hyperion.WorkflowExpression._
import com.krux.hyperion.expression.Parameter

class RedshiftUnloadActivitySpec extends WordSpec {

  "RedshiftUnloadActivity" should {

    implicit val hc: HyperionContext = new HyperionContext(ConfigFactory.load("example"))

    val ec2 = Ec2Resource()

    val awsAccessKeyId = Parameter("AwsAccessKeyId", "someId").encrypted
    val awsAccessKeySecret = Parameter("AwsAccessKeySecret", "someSecret").encrypted

    object MockRedshift extends RedshiftDatabase {
      val id = PipelineObjectId.fixed("_MockRedshift")
      val name = id
      val clusterId = "mock-redshift"
      val username = "mockuser"
      val `*password` = "mockpass"
      val databaseName = "mock_db"
    }

    "Produce the correct unload script" in {
      val testingQuery = """
        |select * from t where
        |id = 'myid'
        |and {tim'e} = #{format(@actualRunTime, 'yyyy-MM-dd')}
        |and some{OtherWeird'Forma}t = #{"{ } a'dfa {" + ' { ex"aef { }'}
        |and name = 'abcdefg'
        |limit 10""".stripMargin

      val escapedUnloadScript = """
        |UNLOAD ('
        |select * from t where
        |id = \\'myid\\'
        |and {tim\\'e} = #{format(@actualRunTime, 'yyyy-MM-dd')}
        |and some{OtherWeird\\'Forma}t = #{"{ } a'dfa {" + ' { ex"aef { }'}
        |and name = \\'abcdefg\\'
        |limit 10')
        |TO 's3://not-important/'
        |WITH CREDENTIALS AS
        |'aws_access_key_id=#{*my_AwsAccessKeyId};aws_secret_access_key=#{*my_AwsAccessKeySecret}'
      """.stripMargin

      val act = RedshiftUnloadActivity(
          MockRedshift, testingQuery, s3 / "not-important/", awsAccessKeyId, awsAccessKeySecret
        )(ec2)

      assert(act.unloadScript.trim === escapedUnloadScript.trim)

    }
  }
}
