# Changelog

All notable changes to this project will be documented in this file.

# Release (2025-07-31)
## General Highlights
The Amazon Web Services (AWS) Advanced Go Wrapper allows an application to take advantage of the features of clustered Aurora databases.

## Module Highlights
* `https://github.com/aws/aws-advanced-go-wrapper/auth-helpers`: [v1.0.0](auth-helpers/CHANGELOG.md#100---2025-07-31)
* `https://github.com/aws/aws-advanced-go-wrapper/aws-secrets-manager`: [v1.0.0](aws-secrets-manager/CHANGELOG.md#100---2025-07-31)
* `https://github.com/aws/aws-advanced-go-wrapper/awssql`: [v1.0.0](awssql/CHANGELOG.md#100---2025-07-31)
* `https://github.com/aws/aws-advanced-go-wrapper/federated-auth`: [v1.0.0](federated-auth/CHANGELOG.md#100---2025-07-31)
* `https://github.com/aws/aws-advanced-go-wrapper/iam`: [v1.0.0](iam/CHANGELOG.md#100---2025-07-31)
* `https://github.com/aws/aws-advanced-go-wrapper/mysql-driver`: [v1.0.0](mysql-driver/CHANGELOG.md#100---2025-07-31)
* `https://github.com/aws/aws-advanced-go-wrapper/okta`: [v1.0.0](okta/CHANGELOG.md#100---2025-07-31)
* `https://github.com/aws/aws-advanced-go-wrapper/otlp`: [v1.0.0](otlp/CHANGELOG.md#100---2025-07-31)
* `https://github.com/aws/aws-advanced-go-wrapper/pgx-driver`: [v1.0.0](pgx-driver/CHANGELOG.md#100---2025-07-31)
* `https://github.com/aws/aws-advanced-go-wrapper/xray`: [v1.0.0](xray/CHANGELOG.md#100---2025-07-31)

# Release (2025-10-02)

## General Highlights
### :magic_wand: Added
* Read Write Splitting Plugin ([PR #198](https://github.com/aws/aws-advanced-go-wrapper/pull/198)). For more information, see the [documentation](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/using-plugins/UsingTheReadWriteSplittingPlugin.md).
* Blue/Green Deployment Plugin ([PR #211](https://github.com/aws/aws-advanced-go-wrapper/pull/211)). For more information, see the [documentation](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/using-plugins/UsingTheBlueGreenPlugin.md).
* Connect Time Plugin ([PR #241](https://github.com/aws/aws-advanced-go-wrapper/pull/241)).

### :bug: Fixed
* Sliding Expiration Cache to properly dispose of expired or overwritten values ([PR #220](https://github.com/aws/aws-advanced-go-wrapper/pull/220)).
* Limitless Connection Plugin to properly round the load metric values for Limitless transaction routers ([PR #250](https://github.com/aws/aws-advanced-go-wrapper/pull/250)).
* Safe concurrent access to properties across different go-routines and monitors ([Issue #242](https://github.com/aws/aws-advanced-go-wrapper/issues/242)).
* If failover is unsuccessful, the underlying error is returned ([PR #252](https://github.com/aws/aws-advanced-go-wrapper/pull/252)).

## Module Highlights
* `https://github.com/aws/aws-advanced-go-wrapper/auth-helpers`: [v1.0.1](auth-helpers/CHANGELOG.md#101---2025-10-02)
* `https://github.com/aws/aws-advanced-go-wrapper/aws-secrets-manager`: [v1.0.1](aws-secrets-manager/CHANGELOG.md#101---2025-10-02)
* `https://github.com/aws/aws-advanced-go-wrapper/awssql`: [v1.1.0](awssql/CHANGELOG.md#110---2025-10-02)
* `https://github.com/aws/aws-advanced-go-wrapper/federated-auth`: [v1.0.1](federated-auth/CHANGELOG.md#101---2025-10-02)
* `https://github.com/aws/aws-advanced-go-wrapper/iam`: [v1.0.1](iam/CHANGELOG.md#101---2025-10-02)
* `https://github.com/aws/aws-advanced-go-wrapper/mysql-driver`: [v1.0.1](mysql-driver/CHANGELOG.md#101---2025-10-02)
* `https://github.com/aws/aws-advanced-go-wrapper/okta`: [v1.0.1](okta/CHANGELOG.md#101---2025-10-02)
* `https://github.com/aws/aws-advanced-go-wrapper/otlp`: [v1.0.1](otlp/CHANGELOG.md#101---2025-10-02)
* `https://github.com/aws/aws-advanced-go-wrapper/pgx-driver`: [v1.0.1](pgx-driver/CHANGELOG.md#101---2025-10-02)
* `https://github.com/aws/aws-advanced-go-wrapper/xray`: [v1.0.1](xray/CHANGELOG.md#101---2025-10-02)