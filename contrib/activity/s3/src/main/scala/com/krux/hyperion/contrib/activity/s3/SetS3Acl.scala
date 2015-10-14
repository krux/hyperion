package com.krux.hyperion.contrib.activity.s3

import scala.collection.JavaConverters._
import scala.annotation.tailrec

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{ ListObjectsRequest, AccessControlList,
  CannedAccessControlList, Grantee, CanonicalGrantee, EmailAddressGrantee, GroupGrantee, Permission,
  ObjectListing, S3ObjectSummary}
import scopt.OptionParser

/**
 * A more flexible way to set s3 acl other than awscli. It makes sure grants is additive and allows
 * both canned acl as well as normal grants with recursive option.
 */
object SetS3Acl {

  final val S3Protocol = "s3://"

  case class S3Uri(ref: String) {
    require(ref.startsWith(S3Protocol), "S3Uri must start with s3 protocol.")

    val (bucket: String, key: String) = ref.stripPrefix(S3Protocol).split("/", 2) match {
      case Array(b, k) => (b, k)
      case Array(b) => (b, "")
    }

  }

  object CannedAccessControlListFactory {
    def apply(name: String) = name match {
      case "private" => CannedAccessControlList.Private
      case "public-read" => CannedAccessControlList.PublicRead
      case "public-read-write" => CannedAccessControlList.PublicReadWrite
      case "authenticated-read" => CannedAccessControlList.AuthenticatedRead
      case "bucket-owner-read" => CannedAccessControlList.BucketOwnerRead
      case "bucket-owner-full-control" => CannedAccessControlList.BucketOwnerFullControl
      case "log-delivery-write" => CannedAccessControlList.LogDeliveryWrite
    }
  }

  object PermissionFactory {
    def apply(name: String) = name match {
      case "full" => Permission.FullControl
      case "read" => Permission.Read
      case "readacl" => Permission.ReadAcp
      case "write" => Permission.Write
      case "writeacl" => Permission.WriteAcp
    }
  }

  case class Grant(
    permission: Permission,
    grantee: Grantee
  )

  object Grant {
    def apply(grantString: String) = {
      grantString.split('=') match {
        case Array(permissionString, "id", canonicalId) =>
          new Grant(PermissionFactory(permissionString), new CanonicalGrantee(canonicalId))
        case Array(permissionString, "emailaddress", emailAddress) =>
          new Grant(PermissionFactory(permissionString), new EmailAddressGrantee(emailAddress))
        case Array(permissionString, "group", groupUri) =>
          new Grant(PermissionFactory(permissionString), GroupGrantee.parseGroupGrantee(groupUri))
        case _ =>
          throw new RuntimeException("Invalid grant expression")
      }
    }
  }

  case class Cli(
    acl: Seq[CannedAccessControlList] = Seq(),
    grants: Seq[Grant] = Seq(),
    recursive: Boolean = false,
    s3Uri: String = ""
  )

  implicit val cannedAclRead: scopt.Read[CannedAccessControlList] =
    scopt.Read.reads(CannedAccessControlListFactory.apply)
  implicit val grantsRead: scopt.Read[Grant] = scopt.Read.reads(Grant.apply)

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Cli]("hyperion-s3-acl-activity") {
      head("hyperion-s3-acl-activity")
      opt[Seq[CannedAccessControlList]]("acl").action((x, c) => c.copy(acl = c.acl ++ x))
        .valueName("acl1,acl2")
        .unbounded
      opt[Seq[Grant]]("grants").action((x, c) => c.copy(grants = c.grants ++ x))
        .valueName("Permission=Grantee_Type=Grantee_ID[,Permission=Grantee_Type=Grantee_ID ...")
        .unbounded
      opt[Unit]("recursive").action((_, c) => c.copy(recursive = true))
      arg[String]("s3uri").action((x, c) => c.copy(s3Uri = x))
    }

    parser.parse(args, Cli()) match {
      case Some(cli) => apply(cli)
      case None => throw new RuntimeException("Invalid cli format")
    }

  }

  def apply(cli: Cli): Unit = {

    val s3Client = new AmazonS3Client()

    val acls = cli.acl
    val grants = cli.grants

    def applyNonRecursive(bucketName: String, key: String) = {
      // first set the canned acl
      acls.foreach { acl =>
        s3Client.setObjectAcl(bucketName, key, acl)
      }

      // then set the grants acl
      val newAcl = grants.foldLeft(s3Client.getObjectAcl(bucketName, key)) { (resultAcl, newAcl) =>
        resultAcl.grantPermission(newAcl.grantee, newAcl.permission)
        resultAcl
      }

      s3Client.setObjectAcl(bucketName, key, newAcl)
    }

    @tailrec
    def applyRecursive(
        bucketName: String,
        prefix: String,
        previousBatch: Option[ObjectListing] = None
      ): Unit = {
      previousBatch match {
        case Some(previousListing) =>
          if (previousListing.isTruncated()) {
            val objListing = s3Client.listNextBatchOfObjects(previousListing)
            objListing.getObjectSummaries().asScala.foreach { summary =>
              applyNonRecursive(summary.getBucketName(), summary.getKey())
            }
          } // Do nothing if the previous is not truncated
        case None =>
          val objListing = s3Client.listObjects(bucketName, prefix)
          objListing.getObjectSummaries().asScala.foreach { summary =>
            applyNonRecursive(summary.getBucketName(), summary.getKey())
          }
          applyRecursive(bucketName, prefix, Option(objListing))
      }
    }

    val s3Uri = S3Uri(cli.s3Uri)

    if (cli.recursive) {
      applyRecursive(s3Uri.bucket, s3Uri.key)
    } else {
      applyNonRecursive(s3Uri.bucket, s3Uri.key)
    }
  }

}
