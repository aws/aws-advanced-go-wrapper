# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/#semantic-versioning-200).

## [1.0.0] - 2025-07-31
* The AWS Advanced Go Wrapper wraps the [pgx - PostgreSQL Driver and Toolkit](https://github.com/jackc/pgx) to connect to PostgreSQL, Aurora PostgreSQL, and RDS PostgreSQL databases. For more information on how to configure and use the `pgx` driver with the AWS Advanced Go Wrapper, see [Using The Go Wrapper](../docs/user-guide/UsingTheGoWrapper.md).  

## [1.0.1] - 2025-10-08
### :bug: Fixed
* Safe concurrent access to properties across different go-routines and monitors ([Issue #242](https://github.com/aws/aws-advanced-go-wrapper/issues/242)).

## [1.0.2] - 2025-10-17
### :crab: Changed
* Update dependency `github.com/aws/aws-advanced-go-wrapper/awssql` to v1.1.1

## [1.0.3] - 2025-12-04
### :crab: Changed
* Update dependency `github.com/aws/aws-advanced-go-wrapper/awssql` to v1.2.0

## [1.0.4] - 2025-12-16
### :crab: Changed
* Update dependency `github.com/aws/aws-advanced-go-wrapper/awssql` to v1.3.0

[1.0.0]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/pgx-driver/1.0.0
[1.0.1]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/pgx-driver/1.0.1
[1.0.2]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/pgx-driver/1.0.2
[1.0.3]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/pgx-driver/1.0.3
[1.0.4]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/pgx-driver/1.0.4
