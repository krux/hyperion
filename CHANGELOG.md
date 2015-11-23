# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

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

