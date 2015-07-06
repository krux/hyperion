# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## 1.14.0 - 2015-07-06
### Added
- #65 - Ability to use roles via STS assume-role

## 1.13.0 - 2015-07-06
### Changed
- #68 - No longer specify AWS keys in configuration for RedshiftUnloadActivity - now must specify as arguments to activity

## 1.12.1 - 2015-07-03
### Fixed
- #74 - DataNode should return path using toString

## 1.12.0 - 2015-07-02
### Added
- #64 - Supports non-default region
### Fixed
- #69 - Role and ResourceRole were not getting properly defaulted on resources

## 1.11.0 - 2015-07-01
### Added
- #4 - Added S3DistCpActivity

## 1.10.1 - 2015-06-22
### Fixed
- #63 - ActionOn* and SchedulerType case objects properly inherit from trait

## 1.10.0 - 2015-06-22
### Added
- #62 - role and resourceRole to EmrCluster types as well as additional missing properties

## 1.9.0 - 2015-06-17
### Added
- #59 - workflow DSL

## 1.8.1 - 2015-06-09
### Changed
- #54 - with* methods that take a sequence are now additive, and replaced withColumns(Seq[String]) with withColumns(String...)
- #56 - reorganize objects into packages by type

## 1.7.2 - 2015-04-28
### Fixed
- #50 - In ShellCommandActivity, make command and scriptUri Either
- #51 - When taskInstanceCount == 0 need to make sure other taskInstance parameters are set to None

## 1.7.1 - 2015-04-28
### Fixed
- #48 - Pipeline blows up if sns.topic is not set

## 1.7.0 - 2015-04-28
### Changed
- #46 - Support remaining properties on resources
- #45 - Support VPC by adding subnetId
- Use Option to construct options instead of Some

## 1.6.2 - 2015-04-26
### Changed
- #40 - Hyperion CLI continue retry to delete the pipeline when --force is used

## 1.6.1 - 2015-04-15
### Changed
- #41 - Refactor Option to Option[Seq] functions

## 1.6.0 - 2015-04-05
### Changed
- #17 - Make pipeline id transparent
- #35 - Use immutable Map for objects building

## 1.5.1 - 2015-04-02
### Added
- #33 - Added support for tags

## 1.5.0 - 2015-04-02
### Added
- #6 - Support remaining schedule aspects

## 1.4.0 - 2015-04-01
### Added
- #14 - Make datapipelineDef be able to have an CLI and remove the Hyperion executable

## 1.3.0 - 2015-04-01
### Added
- #5 - Support parameters

## 1.2.0 - 2015-03-31
### Fixed
- #26 - ShellCommandActivity input and output should actually be a sequence of DataNodes.

## 1.1.0 - 2015-03-31
### Added
- #10 - Support Preconditions
- #18 - Add additional activities to EC2Resource and EmrCluster

### Changed
- #18 - Renamed runCopyActivity on EC2Resource to runCopy

## 1.0.5 - 2015-03-29
### Added
- #13 - Support SQL related databases and the relevant data nodes

## 1.0.4 - 2015-03-29
### Added
- #20  - Support Actions

## 1.0.3 - 2015-03-28
### Added
- #9 - Additional activity types (PigActivity, HiveActivity, HiveCopyActivity, CopyActivity)

## 1.0.2 - 2015-03-27
### Fixed
- #15 - downgrade json4s to 3.2.10

## 1.0.1 - 2015-03-26
### Fixed
- #11 - Spark and MapReduce should dependOn PipelineActivity

## 1.0.0 - 2015-03-25
### Changed
- First public release

