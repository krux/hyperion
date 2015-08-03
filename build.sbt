val awsSdkVersion = "1.9.35"

val nscalaTimeArtifact      = "com.github.nscala-time" %% "nscala-time"               % "1.8.0"
val json4sJacksonArtifact   = "org.json4s"             %% "json4s-jackson"            % "3.2.10"
val scoptArtifact           = "com.github.scopt"       %% "scopt"                     % "3.3.0"
val jschArtifact            = "com.jcraft"             %  "jsch"                      % "0.1.53"
val configArtifact          = "com.typesafe"           %  "config"                    % "1.2.1"
val commonsIoArtifact       = "commons-io"             %  "commons-io"                % "2.4"
val awsDatapipelineArtifact = "com.amazonaws"          %  "aws-java-sdk-datapipeline" % awsSdkVersion
val awsStsArtifact          = "com.amazonaws"          %  "aws-java-sdk-sts"          % awsSdkVersion
val scalatestArtifact       = "org.scalatest"          %% "scalatest"                 % "2.2.4"  % "test"
val mailArtifact            = "com.sun.mail"           %  "mailapi"                   % "1.5.4"
val smtpArtifact            = "com.sun.mail"           %  "smtp"                      % "1.5.4"

val hyperionVersion = "2.0.0"

// Import default settings. This changes `publishTo` settings to use the Sonatype repository and add several commands for publishing.
import SonatypeKeys._

sonatypeSettings

licenses += ("Apache-2.0", url("http://opensource.org/licenses/Apache-2.0"))

// Publishing stuff for Sonatype
publishMavenStyle := true

pomIncludeRepository := { _ => false }

publishTo <<= version { _.endsWith("SNAPSHOT") match {
    case true  => Some("snapshots" at "https://oss.sonatype.org/content/repositories/snapshots")
    case false => Some("releases" at "https://oss.sonatype.org/service/local/staging/deploy/maven2")
  }
}

pgpSecretRing := file("secring.asc")

pgpPublicRing := file("pubring.asc")

pomExtra := (
<url>https://github.com/krux/hyperion</url>
<scm>
   <url>git@github.com:krux/hyperion.git</url>
   <connection>scm:git:git@github.com:krux/hyperion.git</connection>
</scm>
<developers>
   <developer>
     <id>realstraw</id>
     <name>Kexin Xie</name>
     <url>http://github.com/realstraw</url>
   </developer>
   <developer>
     <id>sethyates</id>
     <name>Seth Yates</name>
     <url>http://github.com/sethyates</url>
   </developer>
</developers>
)

// Scaladoc publishing stuff
site.settings

ghpages.settings

git.remoteRepo := "git@github.com:krux/hyperion.git"

site.includeScaladoc()

lazy val commonSettings = Seq(
  organization := "com.krux",
  version := hyperionVersion,
  scalaVersion := "2.11.7",
  crossScalaVersions := Seq(
    "2.10.4",
    "2.11.7"
  ),
  scalacOptions ++= Seq(
    "-deprecation",
    "-feature",
    "-Xlint",
    "-Xfatal-warnings"
  ),
  scalacOptions in (Compile, doc) <++= baseDirectory.map { (bd: File) =>
    Seq(
      "-sourcepath",
      bd.getAbsolutePath,
      "-doc-source-url",
      "https://github.com/krux/hyperion/tree/master/€{FILE_PATH}.scala")
  },
  test in assembly := {} // skip test during assembly
)

lazy val artifactSettings = commonSettings ++ Seq(
  artifact in (Compile, assembly) := {
    val art = (artifact in (Compile, assembly)).value
    art.copy(`classifier` = Some("assembly"))
  },
  addArtifact(artifact in (Compile, assembly), assembly)
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(name := "hyperion").
  dependsOn(
    core,
    contribActivityDefinition
  ).
  aggregate(
    core,
    examples,
    contribActivityDefinition,
    contribActivitySftp,
    contribActivityFile
  )

lazy val core = (project in file("core")).
  settings(commonSettings: _*).
  settings(
    name := "hyperion-core",
    libraryDependencies ++= Seq(
      awsDatapipelineArtifact,
      awsStsArtifact,
      nscalaTimeArtifact,
      json4sJacksonArtifact,
      scoptArtifact,
      configArtifact,
      scalatestArtifact
    )
  )

lazy val examples = (project in file("examples")).
  settings(commonSettings: _*).
  settings(
    name := "hyperion-examples",
    publishArtifact := false,
    libraryDependencies ++= Seq(
      scalatestArtifact
    )
  ).
  dependsOn(core, contribActivityDefinition)

lazy val contribActivityDefinition = (project in file("contrib/activity/definition")).
  settings(commonSettings: _*).
  settings(
    name := "hyperion-activities"
  ).
  dependsOn(core)

lazy val contribActivitySftp = (project in file("contrib/activity/sftp")).
  settings(artifactSettings: _*).
  settings(
    name := "hyperion-sftp-activity",
    libraryDependencies ++= Seq(
      scoptArtifact,
      jschArtifact
    )
  )

lazy val contribActivityFile = (project in file("contrib/activity/file")).
  settings(artifactSettings: _*).
  settings(
    name := "hyperion-file-activity",
    libraryDependencies ++= Seq(
      scoptArtifact,
      commonsIoArtifact
    )
  )

lazy val contribActivityEmail = (project in file("contrib/activity/email")).
  settings(artifactSettings: _*).
  settings(
    name := "hyperion-email-activity",
    libraryDependencies ++= Seq(
      scoptArtifact,
      mailArtifact,
      smtpArtifact
    )
  )

