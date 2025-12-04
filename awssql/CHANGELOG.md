# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/#semantic-versioning-200).

## [1.0.0] - 2025-07-31
* The Amazon Web Services (AWS) Advanced Go Wrapper allows an application to take advantage of the features of clustered Aurora databases.

## [1.1.0] - 2025-10-08
### :magic_wand: Added
* Read Write Splitting Plugin ([PR #198](https://github.com/aws/aws-advanced-go-wrapper/pull/198)). For more information, see the [documentation](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/using-plugins/UsingTheReadWriteSplittingPlugin.md).
* Blue/Green Deployment Plugin ([PR #211](https://github.com/aws/aws-advanced-go-wrapper/pull/211)). For more information, see the [documentation](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/using-plugins/UsingTheBlueGreenPlugin.md).
* Connect Time Plugin ([PR #241](https://github.com/aws/aws-advanced-go-wrapper/pull/241)).

### :bug: Fixed
* Sliding Expiration Cache to properly dispose of expired or overwritten values ([PR #220](https://github.com/aws/aws-advanced-go-wrapper/pull/220)).
* Limitless Connection Plugin to properly round the load metric values for Limitless transaction routers ([PR #250](https://github.com/aws/aws-advanced-go-wrapper/pull/250)).
* Safe concurrent access to properties across different go-routines and monitors ([Issue #242](https://github.com/aws/aws-advanced-go-wrapper/issues/242)).
* If failover is unsuccessful, the underlying error is returned ([PR #252](https://github.com/aws/aws-advanced-go-wrapper/pull/252)).

## [1.1.1] - 2025-10-17
### :crab: Changed
* Refactored PostgresQL statements used by the driver to be fully qualified ([PR #270](https://github.com/aws/aws-advanced-go-wrapper/pull/270)).

## [1.2.0] - 2025-12-04
### :magic_wand: Added
* Aurora Connection Tracker Plugin ([PR #272](https://github.com/aws/aws-advanced-go-wrapper/pull/272)). For more infomration, see the [documentation](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/UsingTheAuroraConnectionTrackerPlugin.md).
* Developer Plugin ([PR #274](https://github.com/aws/aws-advanced-go-wrapper/pull/274)). For more information, see the [documentation](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/using-plugins/UsingTheDeveloperPlugin.md).

### :bug: Fixed
* Blue Green Plugin Status Monitor to poll with the correct rate ([PR #279](https://github.com/aws/aws-advanced-go-wrapper/pull/279)).

[1.0.0]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/awssql/1.0.0
[1.1.0]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/awssql/1.1.0
[1.1.1]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/awssql/1.1.1
[1.2.0]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/awssql/1.2.0
