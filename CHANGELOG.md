# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## 4.4.2 - 2016-09-16
### Added
- [#463](https://github.com/krux/hyperion/issues/463) - Add the number of objects logging back

## 4.4.1 - 2016-08-30
### Fixed
- [#461](https://github.com/krux/hyperion/issues/461) - Fixed GPG encrypt and decrypt on first run

## 4.4.0 - 2016-08-12
### Added
- [#457](https://github.com/krux/hyperion/issues/457) - Add support for "APPEND" insert mode to redshift copy activity

## 4.3.3 - 2016-08-11
### Fixed
- [#455](https://github.com/krux/hyperion/issues/455) - Use emrManaged*SecurityGroupId instead of *SecurityGroupId

## 4.3.2 - 2016-08-11
### Fixed
- [#452](https://github.com/krux/hyperion/issues/452) - EMR cluster when using configuration to specify release label, the ami field is not overriden

## 4.3.1 - 2016-08-10
### Fixed
- [#448](https://github.com/krux/hyperion/issues/448) - Fix a bug that SparkActivity does not work with EMR release label

## 4.3.0 - 2016-08-08
### Fixed
- [#450](https://github.com/krux/hyperion/issues/450) - Update the default AMI to use instance store AMIs instead of EBS backed

## 4.2.0 - 2016-08-05
### Added
- [#289](https://github.com/krux/hyperion/issues/289) - Allow JarActivity to have environment variables and additional classpath JARs

## 4.1.0 - 2016-08-04
### Changed
- [#443](https://github.com/krux/hyperion/issues/443) - Allow HiveActivity to accept multiple input and output parameters

## 4.0.3 - 2016-07-14
### Fixed
- [#439](https://github.com/krux/hyperion/issues/439) - Schedule.ondemand results in pipeline creation failure

## 4.0.2 - 2016-07-14
### Fixed
- [#436](https://github.com/krux/hyperion/issues/436) - PgpActivity should expose withInput, withOutput and markSuccessful

## 4.0.1 - 2016-07-14
### Added
- [#434](https://github.com/krux/hyperion/issues/434) - Allow S3DistCpActivity to receive Parameters.

## 4.0.0 - 2016-07-12

Please refer to the [wiki](https://github.com/krux/hyperion/wiki/Hyperion-v4.0) page for details of migrating from v3 to v4.

### Added
- [#344](https://github.com/krux/hyperion/issues/344) - Add support for defining multiple pipelines with shared schedules within one definition with DataPipelineDefGroup
    - `com.krux.hyperion.HyperionAwsClient` is rewritten and replaced by `com.krux.hyperion.client.AwsClient`
    - `com.krux.hyperion.WorkflowExpression` is moved to `com.krux.hyperion.workflow.WorkflowExpression`
- [#403](https://github.com/krux/hyperion/issues/403) - Updated the default ec2 instance AMI to Amazon Linux AMI 2016.03.2 released on 2016-06-09

### Fixed
- [#356](https://github.com/krux/hyperion/issues/403) - Escape `,` in arguments of Emr and Spark steps

## 3.7.1 - 2016-07-11
### Added
- [#430](https://github.com/krux/hyperion/issues/430) - Add a `--no-check` flag to not check for existence of pipeline before creating

## 3.7.0 - 2016-07-10
### Added
- [#393](https://github.com/krux/hyperion/issues/393) - Ability to add custom properties to pipeline objects
- [#379](https://github.com/krux/hyperion/issues/379) - Cannot override DefaultObject

## 3.6.1 - 2016-07-09
### Added
- [#333](https://github.com/krux/hyperion/issues/333) - Allow explicitly converting an Activity to a WorkflowExpression

## 3.6.0 - 2016-07-09
### Fixed
- [#380](https://github.com/krux/hyperion/issues/380) - Support EMR release label 4.x

## 3.5.2 - 2016-07-08
### Added
- [#374](https://github.com/krux/hyperion/issues/374) - PgpEncryptActivity to encrypt files using GNU implementation of OpenPGP
- [#375](https://github.com/krux/hyperion/issues/375) - PgpDecryptActivity to decrypt OpenPGP-encrypted files

## 3.5.1 - 2016-06-29
### Fixed
- [#420](https://github.com/krux/hyperion/issues/420) - Fix the incorrect retry message

## 3.5.0 - 2016-06-29
### Changed
- [#418](https://github.com/krux/hyperion/issues/418) - Max retry should be configurable and use exponential backoff and jitter instead of fixed interval

## 3.4.2 - 2016-06-28
### Fixed
- [#416](https://github.com/krux/hyperion/issues/416) - SparkStep and SparkTaskActivity needs to be able to pass a HdfsUri to withArguments()

## 3.4.1 - 2016-06-21
### Fixed
- [#404](https://github.com/krux/hyperion/issues/404) - PythonActivity script now uses the correct virtualenv path

## 3.4.0 - 2016-06-20
### Added
- [#410](https://github.com/krux/hyperion/issues/410) - support all redshift unload options for RedshiftUnloadActivity

## 3.3.3 - 2016-05-22
### Added
- [#401](https://github.com/krux/hyperion/issues/401) - Handle `.compare(_)` on parameters without default values

## 3.3.2 - 2016-05-23
### Added
- [#398](https://github.com/krux/hyperion/issues/397) - Add support for securityGroupIds in Ec2Resource

## 3.3.1 - 2016-05-20
## Added
- [#388](https://github.com/krux/hyperion/issues/388) - Make hyperion.log.uri optional

## 3.3.0 - 2016-05-17
### Added
- [#397](https://github.com/krux/hyperion/issues/397) - Ability to set maxActiveInstances optional field for activities

## 3.2.14 - 2016-05-12
### Fixed
- [#394](https://github.com/krux/hyperion/issues/394) - Allow using both s3 and hdfs URIs in S3DistCpActivity

## 3.2.13 - 2016-05-04
## Changed
- [#390](https://github.com/krux/hyperion/issues/390) - Show more detail on validation errors and warnings

## 3.2.12 - 2016-04-27
### Changed
- [#373](https://github.com/krux/hyperion/issues/373) - CLI and the aws client should retry with some delay on throttling exception

### Refactored
- [#386](https://github.com/krux/hyperion/issues/386) - Use JavaConverters instead of JavaConversions

## 3.2.11 - 2016-04-26
### Fixed
- [#381](https://github.com/krux/hyperion/issues/381) - S3DistCpActivity fails when using emr-release label 4.X

## 3.2.10 - 2016-04-22
### Added
- [#376](https://github.com/krux/hyperion/issues/376) - Adds Multiple EmrConfiguration Support

## 3.2.9 - 2016-04-20
### Added
- [#370](https://github.com/krux/hyperion/issues/370) - The standard bootstrap script now fails on older version of AMI (< 3.x)

## 3.2.8 - 2016-04-19
### Added
- [#365](https://github.com/krux/hyperion/issues/365) - Change standard bootstrap action to use aws cli instead of hadoop

## 3.2.7 - 2016-04-16
### Added
- [#362](https://github.com/krux/hyperion/issues/362) - Do not emit empty arrays for EmrConfiguration properties

## 3.2.6 - 2016-04-15
### Added
- [#360](https://github.com/krux/hyperion/issues/360) - Unable to create MapReduceCluster with release Label

## 3.2.5 - 2016-04-11
### Added
- [#357](https://github.com/krux/hyperion/issues/357) - Add recursive option to GoogleStorageDownloadActivity

## 3.2.4 - 2016-03-29
### Added
- [#321](https://github.com/krux/hyperion/issues/321) - Add overloaded methods accepting HS3Uri for Activities

## 3.2.3 - 2016-03-28
### Added
- [#351](https://github.com/krux/hyperion/issues/351) - DeleteS3PathActivity - add option to check for existence of S3 path

## 3.2.2 - 2016-03-16
### Added
- [#349](https://github.com/krux/hyperion/issues/349) - SplitMergeFilesActivity needs to pass `temporary-directory`

## 3.2.1 - 2016-02-13
### Added
- [#345](https://github.com/krux/hyperion/issues/345) - The default alarm message now contains a link to the pipeline

## 3.2.0 - 2016-02-13
### Added
- [#337](https://github.com/krux/hyperion/issues/337) - Implement SendSlackMessage

## 3.1.3 - 2016-02-10
### Changed
- [#335](https://github.com/krux/hyperion/issues/335) - WorkflowGraphRenderer use name instead of id

## 3.1.2 - 2016-02-01
### Added
- [#323](https://github.com/krux/hyperion/issues/323) - Extend `DateTimeExp` to include `format`

## 3.1.1 - 2016-01-28
### Fixed
- [#327](https://github.com/krux/hyperion/issues/327) - Add a --param option to handle override parameters with a comma in the value
- [#328](https://github.com/krux/hyperion/issues/328) - SftpActivity was broken in 3.0 - hard-coded to 'download'

## 3.1.0 - 2016-01-27
### Fixed
- [#324](https://github.com/krux/hyperion/issues/324) - Workflow should be evaluated at the last minute possible

## 3.0.7 - 2016-01-26
### Fixed
- [#318](https://github.com/krux/hyperion/issues/318) - SendFlowdockMessageActivity should use the corresponding HType in apply
- [#320](https://github.com/krux/hyperion/issues/320) - A few shell command based activity is missing input / output

## 3.0.6 - 2016-01-21
### Fixed
- [#315](https://github.com/krux/hyperion/issues/315) - fixed a bug input and output reference in CopyActivity is not included

## 3.0.5 - 2016-01-20
### Added
- [#313](https://github.com/krux/hyperion/issues/313) - added option to startThisHourAt schedule

## 3.0.4 - 2016-01-19
### Fixed
- [#310](https://github.com/krux/hyperion/issues/310) - fix a bug where preconditions missing the referenced objects

## 3.0.3 - 2016-01-14
### Fixed
- [#213](https://github.com/krux/hyperion/issues/213) - Start use the `name` field instead of forcing `id` and `name` to be the same

## 3.0.2 - 2016-01-10
### Added
- [#304](https://github.com/krux/hyperion/issues/304) - Add the missing options to preconditions

## 3.0.1 - 2016-01-09
### Added
- [#300](https://github.com/krux/hyperion/issues/300) - value option in encrypted and unencrypted method to create new parameters through the Parameter object

### Fixed
- [#299](https://github.com/krux/hyperion/issues/299) - Fixes ConstantExpression implicits to avoid unnecessary import
- [#298](https://github.com/krux/hyperion/issues/295) - Make sequence of native type to sequence of HType implicitly available

## 3.0.0 - 2016-01-08
### Changed
- [#295](https://github.com/krux/hyperion/issues/295) - Refactor parameter with adhoc polymorphism with type class instead of reflection TypeTags
- [#248](https://github.com/krux/hyperion/issues/248) - Refactor parameter to have EncryptedParameter and UnencryptedParameter
- [#281](https://github.com/krux/hyperion/issues/281) - Support for not failing on un-defined pipeline parameters
- [#291](https://github.com/krux/hyperion/issues/291) - Clean up the implicits
- [#285](https://github.com/krux/hyperion/issues/285) - SnsAlarm requires topic arn and added default subject and message
- [#286](https://github.com/krux/hyperion/issues/286) - Fix a bug in 3.0 that main class in jar activity is incorrect
- [#282](https://github.com/krux/hyperion/issues/282) - Add support for getting hyperion aws client by pipeline name
- [#280](https://github.com/krux/hyperion/issues/280) - Upgrade to scala 2.10.6
- [#243](https://github.com/krux/hyperion/issues/243) - Revisit and refactor expression and parameter
  - The actionOnTaskFailure and actionOnResource failure is removed from emr activities, they do not belong there.
  - Database objects are changed to be consistent with other objects, this means that one needs to initialize a database object instead of extending a trait
  - Removed hadoopQueue from `HiveCopyActivity` and `PigActivity` as it is not documented by AWS
  - `SparkJobActivity` is renamed to `SparkTaskActivity` to be consistent with the `preActivityTaskConfig` field for similar activity naming from AWS
- [#271](https://github.com/krux/hyperion/issues/271) - Separate CLI with DataPipelineDef

### Added
- [#214](https://github.com/krux/hyperion/issues/214) - Extend CLI to be able to read parameters to be passed from pipeline

## 2.16.7 - 2016-01-05
### Fixed
- [#291](https://github.com/krux/hyperion/issues/291) - Upgrade AWS SDK to 1.10.43

## 2.16.6 - 2015-12-14
### Fixed
- [#277](https://github.com/krux/hyperion/issues/277) - InsertTableQuery actually needs the values placeholders

## 2.16.5 - 2015-12-11
### Fixed
- [#275](https://github.com/krux/hyperion/issues/275) - Schedule is not honouring settings in non-application.conf config

## 2.16.4 - 2015-11-23
### Fixed
- [#273](https://github.com/krux/hyperion/issues/273) - Add `ACCEPTINVCHARS` and the rest of Data Conversion Parameters to redshift copy options

## 2.16.3 - 2015-11-11
### Fixed
- [#269](https://github.com/krux/hyperion/issues/269) - Sftp download auth cancel when using username and password
- [#267](https://github.com/krux/hyperion/issues/267) - Passing 0 to stopAfter should reset end to None

## 2.16.2 - 2015-11-09
### Fixed
- [#264](https://github.com/krux/hyperion/issues/264) - CLI schedule override only the explicitly specified part

## 2.16.1 - 2015-11-09
### Added
- [#262](https://github.com/krux/hyperion/issues/262) - Add slf4j-simple to examples

## 2.16.0 - 2015-11-08
### Added
- [#240](https://github.com/krux/hyperion/issues/240) - Support EmrConfiguration and Property
- [#241](https://github.com/krux/hyperion/issues/241) - Support HttpProxy
- [#255](https://github.com/krux/hyperion/issues/255) - Provide explanations for CLI options
- [#256](https://github.com/krux/hyperion/issues/256) - Use a logging framework instead of println

## 2.15.0 - 2015-11-06
### Added
- [#209](https://github.com/krux/hyperion/issues/209) - Override start activation time on command line

## 2.14.2 - 2015-10-30
### Added
- [#249](https://github.com/krux/hyperion/issues/249) - Implement a simpleName value on MainClass to get just the class name itself
- [#252](https://github.com/krux/hyperion/issues/252) - Add option to Graph to exclude data nodes (or make it the default)

### Fixed
- [#251](https://github.com/krux/hyperion/issues/251) - Graph still emits resources (just not resource dependencies) when not using --include-resources

## 2.14.1 - 2015-10-29
### Fixed
- [#224](https://github.com/krux/hyperion/issues/224) - Add more redshift copy options
- [#225](https://github.com/krux/hyperion/issues/225) - Make HyperionAwsCli fail fast

## 2.14.0 - 2015-10-27
### Added
- [#239](https://github.com/krux/hyperion/issues/239) - Capability to generate graph of workflow

## 2.13.2 - 2015-10-27
### Added
- [#237](https://github.com/krux/hyperion/issues/237) - Allow Spark*Activity to override driver-memory

## 2.13.1 - 2015-10-26
### Added
- [#234](https://github.com/krux/hyperion/issues/234) - SplitMergeFiles should allow ignoring cases where there is no input files

## 2.13.0 - 2015-10-15
### Added
- [#224](https://github.com/krux/hyperion/issues/224) - Spark*Activity should allow setting parameters for spark jobs

## 2.12.3 - 2015-10-14
### Reverted
 - [#229](https://github.com/krux/hyperion/issues/229) - Convert S3DistCpActivity to a HadoopActivity instead of EmrActivity

## 2.12.2 - 2015-10-14
### Added
- [#229](https://github.com/krux/hyperion/issues/229) - Convert S3DistCpActivity to a HadoopActivity instead of EmrActivity
- [#228](https://github.com/krux/hyperion/issues/228) - Allow specifying options to S3DistCpActivity

## 2.12.1 - 2015-10-14
### Fixed
- [#226](https://github.com/krux/hyperion/issues/226) - Improves SetS3AclActivity with canned acl enum and more flexible apply

## 2.12.0 - 2015-10-14
### Added
- [#223](https://github.com/krux/hyperion/issues/223) - Contrib activity that sets S3 ACL

## 2.11.3 - 2015-10-12
### Fixed
- [#220](https://github.com/krux/hyperion/issues/220) - Make SparkActivity download jar to different directory to avoid race condition of jobs running in parallel.

## 2.11.2 - 2015-10-08
### Fixed
- [#217](https://github.com/krux/hyperion/issues/217) - DateTimeExpression methods returns the wrong expression.

## 2.11.1 - 2015-09-29
### Fixed
- [#211](https://github.com/krux/hyperion/issues/211) - RedhishiftUnloadActivity fail when containing expressions with `'`

## 2.11.0 - 2015-09-22
### Fixed
- [#207](https://github.com/krux/hyperion/issues/207) - Make workflow expression DSL avaible to pipeline def by default.

## 2.10.0 - 2015-09-13
### Added
- [#204](https://github.com/krux/hyperion/issues/204) - HadoopActivity and SparkJobActivity should support input and output data nodes

## 2.9.2 - 2015-09-13
### Fixed
- [#202](https://github.com/krux/hyperion/issues/202) - WorkflowGraph fails with assertion if not using named

## 2.9.1 - 2015-09-11
### Fixed
- [#200](https://github.com/krux/hyperion/issues/200) - SendEmailActivity must allow setting of debug and starttls

## 2.9.0 - 2015-09-03
### Added
- [#191](https://github.com/krux/hyperion/issues/191) - Create a SparkActivity-type step that runs a single step using HadoopActivity instead of MapReduceActivity
- [#160](https://github.com/krux/hyperion/issues/160) - Better SNS alarm format support

### Changed
- [#197](https://github.com/krux/hyperion/issues/197) - Update the default EMR AMI version to 3.7 and Spark version to 1.4.0

## 2.8.1 - 2015-09-03
### Fixed
- [#195](https://github.com/krux/hyperion/issues/195) - RepartitionFile emitting empty files

## 2.8.0 - 2015-09-03
### Added
- [#192](https://github.com/krux/hyperion/issues/192) - StringParameter should have implicit conversion to String

## 2.7.3 - 2015-09-03
### Changed
- [#186](https://github.com/krux/hyperion/issues/186) - Change collection constructors to use `.empty`
- [#188](https://github.com/krux/hyperion/issues/188) - SftpDownloadActivity should obey skip-empty as well and it needs to properly handle empty compressed files
- [#189](https://github.com/krux/hyperion/issues/189) - SftpUploadActivity, SftpDownloadActivity and SplitMergeFilesActivity should be able to write a _SUCCESS file

## 2.7.2 - 2015-09-03
### Fixed
- [#184](https://github.com/krux/hyperion/issues/184) - Properties for new notification activities are not properly exposed in the Activity definition

## 2.7.1 - 2015-09-02
### Changed
- [#181](https://github.com/krux/hyperion/issues/181) - Remove `spark.yarn.user.classpath.first` conf for running Spark

## 2.7.0 - 2015-09-02
### Added
- [#172](https://github.com/krux/hyperion/issues/172) - Create activity to send generic SNS message
- [#173](https://github.com/krux/hyperion/issues/173) - Create activity to send generic SQS message
- [#174](https://github.com/krux/hyperion/issues/174) - Create activity to send Flowdock notifications

## 2.6.1 - 2015-09-01
### Fixed
- [179](https://github.com/krux/hyperion/issues/179) - Single quotes in SFTP Activitys date format breaks DataPipeline

## 2.6.0 - 2015-09-01
### Added
- [177](https://github.com/krux/hyperion/issues/177) - The SFTP activity should support a --since to download files since a date

## 2.5.0 - 2015-08-31
### Added
- [175](https://github.com/krux/hyperion/issues/175) - Need to be able to pass options to java in addition to arguments to the main class

## 2.4.0 - 2015-08-30
### Added
- [#164](https://github.com/krux/hyperion/issues/164) - Add support for rdsInstanceId to RdsDatabase
- [#170](https://github.com/krux/hyperion/issues/170) - Output a count of pipeline objects

## 2.3.0 - 2015-08-29
### Fixed
- [#166](https://github.com/krux/hyperion/issues/166) - If the input is empty, split-merge should not create an empty file with headers

### Added
- [#167](https://github.com/krux/hyperion/issues/167) - SftpActivity needs an option to not upload empty files

## 2.2.0 - 2015-08-27
### Fixed
- [#157](https://github.com/krux/hyperion/issues/157) - Use a separate workflow/dependency graph to manage dependency building

## 2.1.1 - 2015-08-23
### Added
- [#162](https://github.com/krux/hyperion/issues/162) - Need way to specify no activity, to allow omitting steps in a workflow expression

## 2.1.0 - 2015-08-21
### Fixed
- [#148](https://github.com/krux/hyperion/issues/148) - Update api doc to use the multi-project build
- [#158](https://github.com/krux/hyperion/issues/158) - Upgrade to scala 2.10.5

## 2.0.12 - 2015-08-17
### Fixed
- [#155](https://github.com/krux/hyperion/issues/155) - Workflow breaks when having ArrowDependency on the right hand side.

## 2.0.11 - 2015-08-17
### Fixed
- [#153](https://github.com/krux/hyperion/issues/153) - The create --force action doesnt detect existing pipelines
  if there are more than 25 active pipelines

## 2.0.10 - 2015-08-14
### Fixed
- [#150](https://github.com/krux/hyperion/issues/150) - The whenMet method returns DataNode instead of S3DataNode

## 2.0.9 - 2015-08-14
### Fixed
- [#149](https://github.com/krux/hyperion/issues/149) - Preconditions are not returned in objects for DataNodes

## 2.0.8 - 2015-08-11
### Fixed
- [#146](https://github.com/krux/hyperion/issues/146) - RepartitionFile doesnt properly add header if creating a single merged file

## 2.0.7 - 2015-08-10
### Fixed
- [#144](https://github.com/krux/hyperion/issues/144) - SplitMergeFileActivity isnt properly compressing final merged output

## 2.0.6 - 2015-08-10
### Fixed
- [#142](https://github.com/krux/hyperion/issues/142) - Arguments to SFTP activity are incorrect

## 2.0.5 - 2015-08-07
### Fixed
- [#140](https://github.com/krux/hyperion/issues/140) - SendEmailActivity runner isnt being published

## 2.0.4 - 2015-08-05
### Fixed
- [#138](https://github.com/krux/hyperion/issues/138) - Make parameter key work for starting letter with lower case

## 2.0.3 - 2015-08-05
### Fixed
- [#136](https://github.com/krux/hyperion/issues/136) - Fix a bug that database object is not included

## 2.0.2 - 2015-08-03
### Added
- [#133](https://github.com/krux/hyperion/issues/133) - SftpActivity needs to support S3 URLs for identity file and download as appropriate

## 2.0.1 - 2015-08-03
### Fixed
- [#131](https://github.com/krux/hyperion/issues/131) - SplitMergeFiles should take strings for bufferSize and bytesPerFile

## 2.0.0 - 2015-08-03
### Added
- [#2](https://github.com/krux/hyperion/issues/2) - Implement SftpUploadActivity
- [#3](https://github.com/krux/hyperion/issues/3) - Implement SftpDownloadActivity
- [#98](https://github.com/krux/hyperion/issues/98) - Add an activity to use SES to send emails rather than mailx
- [#103](https://github.com/krux/hyperion/issues/103) - Provide an activity to split files
- [#107](https://github.com/krux/hyperion/issues/107) - Support Worker Groups
- [#108](https://github.com/krux/hyperion/issues/108) - Add attemptTimeout
- [#109](https://github.com/krux/hyperion/issues/109) - Add lateAfterTimeout
- [#110](https://github.com/krux/hyperion/issues/110) - Add maximumRetries
- [#111](https://github.com/krux/hyperion/issues/111) - Add retryDelay
- [#112](https://github.com/krux/hyperion/issues/112) - Add failureAndRerunMode
- [#115](https://github.com/krux/hyperion/issues/115) - Add ShellScriptConfig
- [#116](https://github.com/krux/hyperion/issues/116) - Add HadoopActivity
- [#125](https://github.com/krux/hyperion/issues/125) - Support collections on WorkflowExpression
- [#127](https://github.com/krux/hyperion/issues/127) - Better type safety for MainClass

### Changed
- [#106](https://github.com/krux/hyperion/issues/106) - Upgrade to Scala 2.11.7
- [#113](https://github.com/krux/hyperion/issues/113) - Reorder parameters for consistency
- [#114](https://github.com/krux/hyperion/issues/114) - Move non-core activities to a contrib project
- [#117](https://github.com/krux/hyperion/issues/117) - Better type safety for PipelineObjectId
- [#118](https://github.com/krux/hyperion/issues/118) - Better type safety for DpPeriod
- [#119](https://github.com/krux/hyperion/issues/119) - Better type safety for S3 URIs
- [#120](https://github.com/krux/hyperion/issues/120) - Better type safety for scripts/scriptUris
- [#121](https://github.com/krux/hyperion/issues/121) - RedshiftUnloadActivitys Access Key Id/Secret be encrypted StringParameters
- [#122](https://github.com/krux/hyperion/issues/122) - AdpS3DataNode should be a 1:1 match to AWS objects
- [#123](https://github.com/krux/hyperion/issues/123) - Rename S3DataNode.fromPath to apply
- [#128](https://github.com/krux/hyperion/issues/128) - Schedule to be constructed via cron/timeSeries/onceAtActivation
- [#129](https://github.com/krux/hyperion/issues/129) - Merge ExpressionDSL into Expression classes and expand functions available
- [#130](https://github.com/krux/hyperion/issues/130) - Rename DateTimeRef to RuntimeSlot to denote real uses

## 1.19.1 - 2015-07-23
### Fixed
- [#99](https://github.com/krux/hyperion/issues/99) - Hyperion CLI driver should exit with appropriate error codes

## 1.19.0 - 2015-07-23
### Fixed
- [#91](https://github.com/krux/hyperion/issues/91) - workflow dsl broken when the right hand side of andThen have dependencies. Note that
  `act1 + act2` is no longer the same as `Seq(act1, act2)` any more.

### Added
- [#101](https://github.com/krux/hyperion/issues/101) - Allow workflow DSL to have duplicated activities.

## 1.18.0 - 2015-07-19
### Added
- [#25](https://github.com/krux/hyperion/issues/25) - Added a run-python runner script and PythonActivity
- [#89](https://github.com/krux/hyperion/issues/89) - Added an activity to email input staging folders
- [#90](https://github.com/krux/hyperion/issues/90) - Added an activity to merge input staging folders and upload to output staging folders

## 1.17.0 - 2015-07-15
### Changed
- [#80](https://github.com/krux/hyperion/issues/80) - Change jar-based activities/steps to require a jar
- [#83](https://github.com/krux/hyperion/issues/83) - Remove dependency assertion in WorkflowDSL
- [#84](https://github.com/krux/hyperion/issues/84) - Drop dependsOn and require WorkflowDSL

## 1.16.0 - 2015-07-15
### Fixed
- [#81](https://github.com/krux/hyperion/issues/81) - Regression: --region parameter is now effectively required on non-EC2 instances due to call to `getCurrentRegion`.

## 1.15.0 - 2015-07-14
### Fixed
- [#78](https://github.com/krux/hyperion/issues/78) - Strip trailing $ from MainClass

## 1.14.0 - 2015-07-06
### Added
- [#65](https://github.com/krux/hyperion/issues/65) - Ability to use roles via STS assume-role

## 1.13.0 - 2015-07-06
### Changed
- [#68](https://github.com/krux/hyperion/issues/68) - No longer specify AWS keys in configuration for RedshiftUnloadActivity - now must specify as arguments to activity

## 1.12.1 - 2015-07-03
### Fixed
- [#74](https://github.com/krux/hyperion/issues/74) - DataNode should return path using toString

## 1.12.0 - 2015-07-02
### Added
- [#64](https://github.com/krux/hyperion/issues/64) - Supports non-default region

### Fixed
- [#69](https://github.com/krux/hyperion/issues/69) - Role and ResourceRole were not getting properly defaulted on resources

## 1.11.0 - 2015-07-01
### Added
- [#4](https://github.com/krux/hyperion/issues/4) - Added S3DistCpActivity

## 1.10.1 - 2015-06-22
### Fixed
- [#63](https://github.com/krux/hyperion/issues/63) - ActionOn* and SchedulerType case objects properly inherit from trait

## 1.10.0 - 2015-06-22
### Added
- [#62](https://github.com/krux/hyperion/issues/62) - role and resourceRole to EmrCluster types as well as additional missing properties

## 1.9.0 - 2015-06-17
### Added
- [#59](https://github.com/krux/hyperion/issues/59) - workflow DSL

## 1.8.1 - 2015-06-09
### Changed
- [#54](https://github.com/krux/hyperion/issues/54) - with* methods that take a sequence are now additive, and replaced withColumns(Seq[String]) with withColumns(String...)
- [#56](https://github.com/krux/hyperion/issues/56) - reorganize objects into packages by type

## 1.7.2 - 2015-04-28
### Fixed
- [#50](https://github.com/krux/hyperion/issues/50) - In ShellCommandActivity, make command and scriptUri Either
- [#51](https://github.com/krux/hyperion/issues/51) - When taskInstanceCount == 0 need to make sure other taskInstance parameters are set to None

## 1.7.1 - 2015-04-28
### Fixed
- [#48](https://github.com/krux/hyperion/issues/48) - Pipeline blows up if sns.topic is not set

## 1.7.0 - 2015-04-28
### Changed
- [#46](https://github.com/krux/hyperion/issues/46) - Support remaining properties on resources
- [#45](https://github.com/krux/hyperion/issues/45) - Support VPC by adding subnetId
- Use Option to construct options instead of Some

## 1.6.2 - 2015-04-26
### Changed
- [#40](https://github.com/krux/hyperion/issues/40) - Hyperion CLI continue retry to delete the pipeline when --force is used

## 1.6.1 - 2015-04-15
### Changed
- [#41](https://github.com/krux/hyperion/issues/41) - Refactor Option to Option[Seq] functions

## 1.6.0 - 2015-04-05
### Changed
- [#17](https://github.com/krux/hyperion/issues/17) - Make pipeline id transparent
- [#35](https://github.com/krux/hyperion/issues/35) - Use immutable Map for objects building

## 1.5.1 - 2015-04-02
### Added
- [#33](https://github.com/krux/hyperion/issues/33) - Added support for tags

## 1.5.0 - 2015-04-02
### Added
- [#6](https://github.com/krux/hyperion/issues/6) - Support remaining schedule aspects

## 1.4.0 - 2015-04-01
### Added
- [#14](https://github.com/krux/hyperion/issues/14) - Make datapipelineDef be able to have an CLI and remove the Hyperion executable

## 1.3.0 - 2015-04-01
### Added
- [#5](https://github.com/krux/hyperion/issues/5) - Support parameters

## 1.2.0 - 2015-03-31
### Fixed
- [#26](https://github.com/krux/hyperion/issues/26) - ShellCommandActivity input and output should actually be a sequence of DataNodes.

## 1.1.0 - 2015-03-31
### Added
- [#10](https://github.com/krux/hyperion/issues/10) - Support Preconditions
- [#18](https://github.com/krux/hyperion/issues/18) - Add additional activities to EC2Resource and EmrCluster

### Changed
- [#18](https://github.com/krux/hyperion/issues/18) - Renamed runCopyActivity on EC2Resource to runCopy

## 1.0.5 - 2015-03-29
### Added
- [#13](https://github.com/krux/hyperion/issues/13) - Support SQL related databases and the relevant data nodes

## 1.0.4 - 2015-03-29
### Added
- #20  - Support Actions

## 1.0.3 - 2015-03-28
### Added
- [#9](https://github.com/krux/hyperion/issues/9) - Additional activity types (PigActivity, HiveActivity, HiveCopyActivity, CopyActivity)

## 1.0.2 - 2015-03-27
### Fixed
- [#15](https://github.com/krux/hyperion/issues/15) - downgrade json4s to 3.2.10

## 1.0.1 - 2015-03-26
### Fixed
- [#11](https://github.com/krux/hyperion/issues/11) - Spark and MapReduce should dependOn PipelineActivity

## 1.0.0 - 2015-03-25
### Changed
- First public release

