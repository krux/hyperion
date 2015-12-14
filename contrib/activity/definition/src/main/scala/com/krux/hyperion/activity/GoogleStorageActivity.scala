package com.krux.hyperion.activity

import com.krux.hyperion.adt.{ HString, HBoolean, HS3Uri }
import com.krux.hyperion.aws.AdpShellCommandActivity
import com.krux.hyperion.datanode.S3DataNode
import com.krux.hyperion.resource.{ Resource, Ec2Resource }
import com.krux.hyperion.HyperionContext
import com.krux.hyperion.common.S3Uri

trait GoogleStorageActivity extends BaseShellCommandActivity {

  type Self <: GoogleStorageActivity

  def botoConfigUrl: HS3Uri
  def googleStorageUri: HString

  override def scriptArguments = Seq(botoConfigUrl.serialize: HString, googleStorageUri)

}

object GoogleStorageActivity {

  def downloadScript(implicit hc: HyperionContext) = S3Uri(s"${hc.scriptUri}activities/gsutil-download.sh")

  def uploadScript(implicit hc: HyperionContext) = S3Uri(s"${hc.scriptUri}activities/gsutil-upload.sh")

}
