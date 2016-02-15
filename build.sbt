val hyperionVersion = "3.2.1"
val scala210Version = "2.10.6"
val scala211Version = "2.11.7"
val awsSdkVersion   = "1.10.43"
val mailVersion     = "1.5.4"
val slf4jVersion    = "1.7.12"

val nscalaTimeArtifact      = "com.github.nscala-time" %% "nscala-time"               % "2.6.0"
val jodaConvertArtifact     = "org.joda"               %  "joda-convert"              % "1.7"    % "provided"
val json4sJacksonArtifact   = "org.json4s"             %% "json4s-jackson"            % "3.2.10"
val scoptArtifact           = "com.github.scopt"       %% "scopt"                     % "3.3.0"
val jschArtifact            = "com.jcraft"             %  "jsch"                      % "0.1.53"
val configArtifact          = "com.typesafe"           %  "config"                    % "1.2.1"
val commonsIoArtifact       = "commons-io"             %  "commons-io"                % "2.4"
val awsDatapipelineArtifact = "com.amazonaws"          %  "aws-java-sdk-datapipeline" % awsSdkVersion
val awsStsArtifact          = "com.amazonaws"          %  "aws-java-sdk-sts"          % awsSdkVersion
val awsS3Artifact           = "com.amazonaws"          %  "aws-java-sdk-s3"           % awsSdkVersion
val awsSqsArtifact          = "com.amazonaws"          %  "aws-java-sdk-sqs"          % awsSdkVersion
val awsSnsArtifact          = "com.amazonaws"          %  "aws-java-sdk-sns"          % awsSdkVersion
val mailArtifact            = "com.sun.mail"           %  "mailapi"                   % mailVersion
val smtpArtifact            = "com.sun.mail"           %  "smtp"                      % mailVersion
val slf4jApiArtifact        = "org.slf4j"              %  "slf4j-api"                 % slf4jVersion
val slf4jSimpleArtifact     = "org.slf4j"              %  "slf4j-simple"              % slf4jVersion
val scalatestArtifact       = "org.scalatest"          %% "scalatest"                 % "2.2.4"  % "test"

lazy val publishSettings = Seq(
  sonatypeProfileName := "com.krux",
  publishMavenStyle := true,
  pomIncludeRepository := { _ => false },
  pgpSecretRing := file("secring.asc"),
  pgpPublicRing := file("pubring.asc"),
  publishTo := {
    if (isSnapshot.value)
      Some("snapshots" at "https://oss.sonatype.org/content/repositories/snapshots")
    else
      Some("releases" at "https://oss.sonatype.org/service/local/staging/deploy/maven2")
  },
  pomExtra := (
    <url>https://github.com/krux/hyperion</url>
    <scm>
       <url>git@github.com:krux/hyperion.git</url>
       <connection>scm:git:git@github.com:krux/hyperion.git</connection>
    </scm>
    <licenses>
      <license>
        <name>Apache-2.0</name>
        <url>http://opensource.org/licenses/Apache-2.0</url>
      </license>
    </licenses>
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
)

lazy val noPublishSettings = Seq(
  publishArtifact := false,
  publish := (),
  publishLocal := ()
)

lazy val commonSettings = Seq(
  organization := "com.krux",
  version := hyperionVersion,
  scalaVersion := scala211Version,
  crossScalaVersions := Seq(
    scala210Version,
    scala211Version
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
  libraryDependencies += scalatestArtifact,
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
  settings(unidocSettings: _*).
  settings(publishSettings: _*).
  settings(site.settings ++ ghpages.settings: _*).
  settings(
    name := "hyperion",
    site.addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), "latest/api"),
    git.remoteRepo := "git@github.com:krux/hyperion.git"
  ).
  dependsOn(
    core,
    contribActivityDefinition
  ).
  aggregate(
    core,
    examples,
    contribActivityDefinition,
    contribActivitySftp,
    contribActivityEmail,
    contribActivityNotification,
    contribActivityFile,
    contribActivityS3
  )

lazy val core = (project in file("core")).
  enablePlugins(BuildInfoPlugin).
  settings(commonSettings: _*).
  settings(publishSettings: _*).
  settings(
    name := "hyperion-core",
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "com.krux.hyperion",
    libraryDependencies ++= Seq(
      awsDatapipelineArtifact,
      awsStsArtifact,
      nscalaTimeArtifact,
      json4sJacksonArtifact,
      scoptArtifact,
      configArtifact,
      slf4jApiArtifact
    )
  )

lazy val examples = (project in file("examples")).
  settings(commonSettings: _*).
  settings(noPublishSettings: _*).
  settings(
    name := "hyperion-examples",
    libraryDependencies += slf4jSimpleArtifact
  ).
  dependsOn(core, contribActivityDefinition)

lazy val contribActivityDefinition = (project in file("contrib/activity/definition")).
  settings(commonSettings: _*).
  settings(publishSettings: _*).
  settings(
    name := "hyperion-activities"
  ).
  dependsOn(core)

lazy val contribActivitySftp = (project in file("contrib/activity/sftp")).
  settings(artifactSettings: _*).
  settings(publishSettings: _*).
  settings(
    name := "hyperion-sftp-activity",
    libraryDependencies ++= Seq(
      scoptArtifact,
      awsS3Artifact,
      jschArtifact,
      jodaConvertArtifact
    )
  )

lazy val contribActivityFile = (project in file("contrib/activity/file")).
  settings(artifactSettings: _*).
  settings(publishSettings: _*).
  settings(
    name := "hyperion-file-activity",
    libraryDependencies ++= Seq(
      scoptArtifact,
      commonsIoArtifact
    )
  )

lazy val contribActivityEmail = (project in file("contrib/activity/email")).
  settings(artifactSettings: _*).
  settings(publishSettings: _*).
  settings(
    name := "hyperion-email-activity",
    libraryDependencies ++= Seq(
      scoptArtifact,
      mailArtifact,
      smtpArtifact
    )
  )

lazy val contribActivityNotification = (project in file("contrib/activity/notification")).
  settings(artifactSettings: _*).
  settings(publishSettings: _*).
  settings(
    name := "hyperion-notification-activity",
    libraryDependencies ++= Seq(
      scoptArtifact,
      json4sJacksonArtifact,
      awsSnsArtifact,
      awsSqsArtifact,
      smtpArtifact
    )
  )

lazy val contribActivityS3 = (project in file("contrib/activity/s3")).
  settings(artifactSettings: _*).
  settings(publishSettings: _*).
  settings(
    name := "hyperion-s3-activity",
    libraryDependencies ++= Seq(
      scoptArtifact,
      awsS3Artifact
    )
  )
