val scalatestArtifact       = "org.scalatest"          %% "scalatest"                 % "2.2.4"  % "test"
val awsDatapipelineArtifact = "com.amazonaws"          %  "aws-java-sdk-datapipeline" % "1.9.19"
val nscalaTimeArtifact      = "com.github.nscala-time" %% "nscala-time"               % "1.8.0"
val json4sJacksonArtifact   = "org.json4s"             %% "json4s-jackson"            % "3.2.10"
val scoptArtifact           = "com.github.scopt"       %% "scopt"                     % "3.3.0"
val configArtifact          = "com.typesafe"           %  "config"                    % "1.2.1"

import SonatypeKeys._

// Import default settings. This changes `publishTo` settings to use the Sonatype repository and add several commands for publishing.
sonatypeSettings

licenses += ("Apache-2.0", url("http://opensource.org/licenses/Apache-2.0"))

// Publishing stuff for sonatype
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
</developers>
)

// Scaladoc publishing stuff
site.settings

ghpages.settings

git.remoteRepo := "git@github.com:krux/hyperion.git"

site.includeScaladoc()

lazy val commonSettings = Seq(
  organization := "com.krux",
  scalacOptions ++= Seq("-deprecation", "-feature", "-Xlint", "-Xfatal-warnings"),
  version := "1.0.4",
  scalaVersion := "2.11.6",
  crossScalaVersions := Seq("2.10.4", "2.11.6"),
  libraryDependencies += scalatestArtifact,
  scalacOptions in (Compile, doc) <++= baseDirectory.map { (bd: File) =>
    Seq(
      "-sourcepath",
      bd.getAbsolutePath,
      "-doc-source-url",
      "https://github.com/krux/hyperion/tree/master/€{FILE_PATH}.scala")
  },
  test in assembly := {}, // skip test during assembly
  assemblyJarName in assembly := "hyperion",
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(prependShellScript = Some(
    Seq(
      """#!/usr/bin/env bash
        |if [[ $# -ge 2 && $1 == "-jar" ]]; then
        |    shift
        |    ext_jar=$1":"; shift
        |else
        |    ext_jar=""
        |fi
        |exec java -cp $ext_jar$0 com.krux.hyperion.Hyperion $@""".stripMargin
  )))
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "hyperion",
    libraryDependencies ++= Seq(
      awsDatapipelineArtifact,
      nscalaTimeArtifact,
      json4sJacksonArtifact,
      scoptArtifact,
      configArtifact
    )
  )
