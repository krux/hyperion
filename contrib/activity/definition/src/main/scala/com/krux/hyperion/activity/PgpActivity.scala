package com.krux.hyperion.activity

import com.krux.hyperion.HyperionContext
import com.krux.hyperion.common.S3Uri

/**
  * Base trait for server-side PGP encryption activities.
  */
trait PgpActivity extends BaseShellCommandActivity {
  type Self <: PgpActivity
}

object PgpActivity {
  def decryptScript(implicit hc: HyperionContext) = S3Uri(s"${hc.scriptUri}activities/gpg-decrypt.sh")

  def encryptScript(implicit hc: HyperionContext) = S3Uri(s"${hc.scriptUri}activities/gpg-encrypt.sh")
}
