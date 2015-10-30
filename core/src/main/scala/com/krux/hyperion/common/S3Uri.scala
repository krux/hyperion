package com.krux.hyperion.common

/**
 * The S3Uri provides a typesafe way of representing S3 URI's.
 *
 * There are a variety of ways to create S3Uri's.  You don't have to create an instance
 * directly.  You can create using string interpolation such as s3"hyperion-bucket/some-path"
 * or using a builder such as s3 / "hyperion-bucket" / "some-path".
 */
case class S3Uri(ref: String) {
  require(ref.startsWith("s3://"), "S3Uri must start with s3 protocol.")

  def /(next: String) = S3Uri(s"$ref/$next")

  override def toString = ref
}

trait S3UriHelper {

  class S3UriBuilder {
    def /(next: String) = s3(next)
  }

  def s3: S3UriBuilder = new S3UriBuilder

  def s3(uri: String*): S3Uri = S3Uri(s"s3://${uri.mkString("/")}")

}

object S3Uri extends S3UriHelper {

  implicit class S3StringContext(val sc: StringContext) extends AnyVal {
    def s3(args: Any*): S3Uri = S3Uri.s3(sc.s(args: _*))
  }

}
