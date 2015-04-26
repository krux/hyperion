# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## Unreleased
### Added
- TODO

## 1.6.2 - 2015-04-26
### Changed
- #40 - Hyperion CLI continue retry to delete the pipeline when --force is used

## 1.6.1 - 2015-04-15
### Changed
- #41 - Refactor Option to Option[Seq] functions

## 1.6.0 - 2015-04-05
### Changed
- #17 - Make pipline id transparent
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

