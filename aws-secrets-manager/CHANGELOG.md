# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/#semantic-versioning-200).

## [1.0.0] - 2025-07-31
* The AWS Secrets Manager Plugin supports usage of database credentials stored as secrets in the AWS Secrets Manager. To see information on how to configure and use AWS Secrets Manager Plugin, see [Using the AWS Secrets Manager Plugin](../docs/user-guide/using-plugins/UsingTheAwsSecretsManagerPlugin.md). 

## [1.0.1] - 2025-10-08
### :bug: Fixed
* Safe concurrent access to properties across different go-routines and monitors ([Issue #242](https://github.com/aws/aws-advanced-go-wrapper/issues/242)).

## [1.0.2] - 2025-10-17
### :crab: Changed
* Update dependency `github.com/aws/aws-advanced-go-wrapper/awssql` to v1.1.1

[1.0.0]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/aws-secrets-manager/1.0.0
[1.0.1]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/aws-secrets-manager/1.0.1
[1.0.2]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/aws-secrets-manager/1.0.2
