# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/#semantic-versioning-200).

## [1.0.0] - 2025-07-31
* The AWS Advanced Go Wrapper supports the use of OpenTelemetry to collect telemetry data. To see information on how to configure and use OpenTelemetry with the AWS Advanced Go Wrapper, see [Telemetry User Guide](../docs/user-guide/Telemetry.md).

## [1.0.1] - 2025-10-08
### :bug: Fixed
* Safe concurrent access to properties across different go-routines and monitors ([Issue #242](https://github.com/aws/aws-advanced-go-wrapper/issues/242)).

[1.0.0]: https://github.com/awslabs/aws-advanced-go-wrapper/releases/tag/otlp/1.0.0
[1.0.1]: https://github.com/awslabs/aws-advanced-go-wrapper/releases/tag/otlp/1.0.1