# Hyperion
[![Build Status](https://secure.travis-ci.org/krux/hyperion.png)](http://travis-ci.org/krux/hyperion)

> In Starcraft, the Hyperion is a Behemoth-class battlecruiser. During the
> Second Great War, Raynor's Raiders made strategic decisions on the
> Hyperion's bridge -- the battlecruiser's command center.

Library and abstractions of AWS data pipeline.

## Problem Statement

This project aims to solve the following problem:

1. Make it easy to define an AWS DataPipeline using a clear, fluent Scala DSL

## Configuration

Add the Sonatype.org Releases repo as a resolver in your `build.sbt` or `Build.scala` as appropriate.

```scala
resolvers += "Sonatype.org Releases" at "https://oss.sonatype.org/content/repositories/releases/"
```

Add hyperion as a dependency in your `build.sbt` or `Build.scala` as appropriate.

```scala
libraryDependencies ++= Seq(
  // Other dependencies ...
  "com.krux" %% "hyperion" % "2.1.0"
)
```

## Scala Versions

This project is compiled, tested, and published for the following Scala versions:

1. 2.10.5
2. 2.11.7

## Usage

### Creating a pipeline

To create a new pipeline, create a Scala class in `com.krux.datapipeline.pipelines`.
Look at [ExampleSpark](src/main/scala/com/krux/hyperion/examples/ExampleSpark.scala) for an example pipeline.

### Manually uploading

To generate a JSON file describing the pipeline, ensure you have created the assembly:
```shell
$ sbt assembly
```

Then, run hyperion with the class name (specify the external jar location if it's not in the
classpath):
```shell
$ ./hyperion [-jar your-jar-implementing-pipelines.jar] your.piplines.ThePipeline generate > ThePipeline.json
```

Then you can go to the [AWS Data Pipeline Management Console](https://console.aws.amazon.com/datapipeline/),
click _Create new pipeline_ and enter the class name for _Name_ and click _Import a definition_ and
select _Load local file_.  Finally, click _Activate_.

### Automatically uploading

To create a pipeline automatically, ensure you have created the assembly:
```shell
$ sbt assembly
```

Then, run hyperion with `create` and the class name:
```shell
$ ./hyperion [-jar your-jar-implementing-pipelines.jar] your.pipeline.ThePipeline create
```

This will use the DataPipeline API to create the pipeline and put the pipeline definition.

### Activating a pipeline

You can activate a pipeline either in the Data Pipeline Management Console, by using the `--activate`
option when using `create` command or by using the `activate` command.

```shell
$ ./hyperion activate df-1234567890
```

## Scaladoc API

The Scaladoc API for this project can be found [here](http://krux.github.io/hyperion/latest/api).

## License

Hyperion is licensed under [APL 2.0](LICENSE).

## Note

Due to AWS data pipeline bug, all schemas involve data pipleine needs be available in the default
search_path.

For more details: https://forums.aws.amazon.com/thread.jspa?threadID=166340

