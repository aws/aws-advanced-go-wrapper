# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/#semantic-versioning-200).

## [1.0.0] - 2025-07-31
* The AWS Advanced Go Wrapper wraps the [Go-MySQL-Driver](https://github.com/go-sql-driver/mysql) to connect to MySQL, Aurora MySQL, and RDS MySQL databases. For more information on how to configure and use the `Go-MySQL` driver with the AWS Advanced Go Wrapper, see [Using The Go Wrapper](../docs/user-guide/UsingTheGoWrapper.md).

## [1.0.1] - 2025-10-08
### :bug: Fixed
* Safe concurrent access to properties across different go-routines and monitors ([Issue #242](https://github.com/aws/aws-advanced-go-wrapper/issues/242)).

[1.0.0]: https://github.com/awslabs/aws-advanced-go-wrapper/releases/tag/mysql-driver/1.0.0
[1.0.1]: https://github.com/awslabs/aws-advanced-go-wrapper/releases/tag/mysql-driver/1.0.1
