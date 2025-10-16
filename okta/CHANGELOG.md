# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/#semantic-versioning-200).

## [1.0.0] - 2025-07-31
* The Okta Authentication Plugin adds support for authentication via Microsoft Active Directory Federation Services (AD FS) and then database access via IAM. To see information on how to configure and use the Okta Authentication Plugin, see [Using the Okta Authentication Plugin](../docs/user-guide/using-plugins/UsingTheOktaAuthPlugin.md).

## [1.0.1] - 2025-10-08
### :bug: Fixed
* Safe concurrent access to properties across different go-routines and monitors ([Issue #242](https://github.com/aws/aws-advanced-go-wrapper/issues/242)).

## [1.0.2] - 2025-10-17
### :crab: Changed
* Update dependency `github.com/aws/aws-advanced-go-wrapper/awssql` to v1.1.1
* Update dependency `github.com/aws/aws-advanced-go-wrapper/auth-helpers` to v1.0.2

[1.0.0]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/okta/1.0.0
[1.0.1]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/okta/1.0.1
[1.0.2]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/okta/1.0.2
