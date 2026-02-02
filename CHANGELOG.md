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

# Release (2025-10-08)

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
* `https://github.com/aws/aws-advanced-go-wrapper/auth-helpers`: [v1.0.1](auth-helpers/CHANGELOG.md#101---2025-10-08)
* `https://github.com/aws/aws-advanced-go-wrapper/aws-secrets-manager`: [v1.0.1](aws-secrets-manager/CHANGELOG.md#101---2025-10-08)
* `https://github.com/aws/aws-advanced-go-wrapper/awssql`: [v1.1.0](awssql/CHANGELOG.md#110---2025-10-08)
* `https://github.com/aws/aws-advanced-go-wrapper/federated-auth`: [v1.0.1](federated-auth/CHANGELOG.md#101---2025-10-08)
* `https://github.com/aws/aws-advanced-go-wrapper/iam`: [v1.0.1](iam/CHANGELOG.md#101---2025-10-08)
* `https://github.com/aws/aws-advanced-go-wrapper/mysql-driver`: [v1.0.1](mysql-driver/CHANGELOG.md#101---2025-10-08)
* `https://github.com/aws/aws-advanced-go-wrapper/okta`: [v1.0.1](okta/CHANGELOG.md#101---2025-10-08)
* `https://github.com/aws/aws-advanced-go-wrapper/otlp`: [v1.0.1](otlp/CHANGELOG.md#101---2025-10-08)
* `https://github.com/aws/aws-advanced-go-wrapper/pgx-driver`: [v1.0.1](pgx-driver/CHANGELOG.md#101---2025-10-08)
* `https://github.com/aws/aws-advanced-go-wrapper/xray`: [v1.0.1](xray/CHANGELOG.md#101---2025-10-08)

# Release (2025-10-17)
## General Highlights
* Refactored PostgresQL statements used by the driver to be fully qualified ([PR #270](https://github.com/aws/aws-advanced-go-wrapper/pull/270)).

## Module Highlights
* `https://github.com/aws/aws-advanced-go-wrapper/auth-helpers`: [v1.0.2](auth-helpers/CHANGELOG.md#102---2025-10-17)
* `https://github.com/aws/aws-advanced-go-wrapper/aws-secrets-manager`: [v1.0.2](aws-secrets-manager/CHANGELOG.md#102---2025-10-17)
* `https://github.com/aws/aws-advanced-go-wrapper/awssql`: [v1.1.1](awssql/CHANGELOG.md#111---2025-10-17)
* `https://github.com/aws/aws-advanced-go-wrapper/federated-auth`: [v1.0.2](federated-auth/CHANGELOG.md#102---2025-10-17)
* `https://github.com/aws/aws-advanced-go-wrapper/iam`: [v1.0.2](iam/CHANGELOG.md#102---2025-10-17)
* `https://github.com/aws/aws-advanced-go-wrapper/mysql-driver`: [v1.0.2](mysql-driver/CHANGELOG.md#102---2025-10-17)
* `https://github.com/aws/aws-advanced-go-wrapper/okta`: [v1.0.2](okta/CHANGELOG.md#102---2025-10-17)
* `https://github.com/aws/aws-advanced-go-wrapper/otlp`: [v1.0.2](otlp/CHANGELOG.md#102---2025-10-17)
* `https://github.com/aws/aws-advanced-go-wrapper/pgx-driver`: [v1.0.2](pgx-driver/CHANGELOG.md#102---2025-10-17)
* `https://github.com/aws/aws-advanced-go-wrapper/xray`: [v1.0.2](xray/CHANGELOG.md#102---2025-10-17)

# Release (2025-12-04)
## General Highlights
### :magic_wand: Added
* Aurora Connection Tracker Plugin ([PR #272](https://github.com/aws/aws-advanced-go-wrapper/pull/272)). For more infomration, see the [documentation](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/UsingTheAuroraConnectionTrackerPlugin.md).
* Developer Plugin ([PR #274](https://github.com/aws/aws-advanced-go-wrapper/pull/274)). For more information, see the [documentation](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/using-plugins/UsingTheDeveloperPlugin.md).
* Custom Endpoint Plugin ([PR #275](https://github.com/aws/aws-advanced-go-wrapper/pull/275)). For more information, see the [documentation](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/using-plugins/UsingTheCustomEndpointPlugin.md).

### :bug: Fixed
* Blue Green Plugin Status Monitor to poll with the correct rate ([PR #279](https://github.com/aws/aws-advanced-go-wrapper/pull/279)).

# Module Highlights
* `https://github.com/aws/aws-advanced-go-wrapper/auth-helpers`: [v1.0.3](auth-helpers/CHANGELOG.md#103---2025-12-04)
* `https://github.com/aws/aws-advanced-go-wrapper/aws-secrets-manager`: [v1.0.3](aws-secrets-manager/CHANGELOG.md#103---2025-12-04)
* `https://github.com/aws/aws-advanced-go-wrapper/awssql`: [v1.2.0](awssql/CHANGELOG.md#120---2025-12-04)
* `https://github.com/aws/aws-advanced-go-wrapper/custom-endpoint`: [v1.2.0](custom-endpoint/CHANGELOG.md#100---2025-12-04)
* `https://github.com/aws/aws-advanced-go-wrapper/federated-auth`: [v1.0.3](federated-auth/CHANGELOG.md#103---2025-12-04)
* `https://github.com/aws/aws-advanced-go-wrapper/iam`: [v1.0.3](iam/CHANGELOG.md#103---2025-12-04)
* `https://github.com/aws/aws-advanced-go-wrapper/mysql-driver`: [v1.0.3](mysql-driver/CHANGELOG.md#103---2025-12-04)
* `https://github.com/aws/aws-advanced-go-wrapper/okta`: [v1.0.3](okta/CHANGELOG.md#103---2025-12-04)
* `https://github.com/aws/aws-advanced-go-wrapper/otlp`: [v1.0.3](otlp/CHANGELOG.md#103---2025-12-04)
* `https://github.com/aws/aws-advanced-go-wrapper/pgx-driver`: [v1.0.3](pgx-driver/CHANGELOG.md#103---2025-12-04)
* `https://github.com/aws/aws-advanced-go-wrapper/xray`: [v1.0.3](xray/CHANGELOG.md#103---2025-12-04)

# Release (2025-12-16)
## General Highlights
### :magic_wand: Added
* Aurora Initial Connection Strategy Plugin ([PR #282](https://github.com/aws/aws-advanced-go-wrapper/pull/282)). For more information, see the [documentation](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/using-plugins/UsingTheAuroraInitialConnectionStrategyPlugin.md).

# Module Highlights
* `https://github.com/aws/aws-advanced-go-wrapper/auth-helpers`: [v1.0.4](auth-helpers/CHANGELOG.md#104---2025-12-16)
* `https://github.com/aws/aws-advanced-go-wrapper/aws-secrets-manager`: [v1.0.4](aws-secrets-manager/CHANGELOG.md#104---2025-12-16)
* `https://github.com/aws/aws-advanced-go-wrapper/awssql`: [v1.3.0](awssql/CHANGELOG.md#130---2025-12-16)
* `https://github.com/aws/aws-advanced-go-wrapper/custom-endpoint`: [v1.0.1](custom-endpoint/CHANGELOG.md#101---2025-12-16)
* `https://github.com/aws/aws-advanced-go-wrapper/federated-auth`: [v1.0.4](federated-auth/CHANGELOG.md#104---2025-12-16)
* `https://github.com/aws/aws-advanced-go-wrapper/iam`: [v1.0.4](iam/CHANGELOG.md#104---2025-12-16)
* `https://github.com/aws/aws-advanced-go-wrapper/mysql-driver`: [v1.0.4](mysql-driver/CHANGELOG.md#104---2025-12-16)
* `https://github.com/aws/aws-advanced-go-wrapper/okta`: [v1.0.4](okta/CHANGELOG.md#104---2025-12-16)
* `https://github.com/aws/aws-advanced-go-wrapper/otlp`: [v1.0.4](otlp/CHANGELOG.md#104---2025-12-16)
* `https://github.com/aws/aws-advanced-go-wrapper/pgx-driver`: [v1.0.4](pgx-driver/CHANGELOG.md#104---2025-12-16)
* `https://github.com/aws/aws-advanced-go-wrapper/xray`: [v1.0.4](xray/CHANGELOG.md#104---2025-12-16)

# Release (2026-02-03)
## General Highlights
### :magic_wand: Added
* New connection properties allowing users to load custom Secret Data format ([PR #310](https://github.com/aws/aws-advanced-go-wrapper/pull/320)), see [Using the AWS Secrets Manager Plugin](../docs/user-guide/using-plugins/UsingTheAwsSecretsManagerPlugin.md) for more details.
* Support for EU, AU, and UK domains ([PR #325](https://github.com/aws/aws-advanced-go-wrapper/pull/325)).

### :bug: Fixed
* Embed messages file to allow it to be bundled in with binary ([Issue #301](https://github.com/aws/aws-advanced-go-wrapper/issues/301)).
* During B/G switchover, ensure IAM host name should be based on green host ([PR #321](https://github.com/aws/aws-advanced-go-wrapper/pull/321)).
* Address various race conditions ([Issue #318](https://github.com/aws/aws-advanced-go-wrapper/issues/318)).
* Stop B/G monitors after switchover completes ([PR #323](https://github.com/aws/aws-advanced-go-wrapper/pull/323)).
* Goroutine to clean up open connections when failover never happens ([PR #327](https://github.com/aws/aws-advanced-go-wrapper/pull/327)).
* Ensure B/G monitors are set up ([PR #330](https://github.com/aws/aws-advanced-go-wrapper/pull/330)). 

### :crab: Changed
* Cache efm2 monitor key for better performance ([PR #328](https://github.com/aws/aws-advanced-go-wrapper/pull/328)).

# Module Highlights
* `https://github.com/aws/aws-advanced-go-wrapper/auth-helpers`: [v1.0.5](auth-helpers/CHANGELOG.md#105---2026-02-03)
* `https://github.com/aws/aws-advanced-go-wrapper/aws-secrets-manager`: [v1.1.0](aws-secrets-manager/CHANGELOG.md#110---2026-02-03)
* `https://github.com/aws/aws-advanced-go-wrapper/awssql`: [v1.4.0](awssql/CHANGELOG.md#140---2026-02-03)
* `https://github.com/aws/aws-advanced-go-wrapper/custom-endpoint`: [v1.0.2](custom-endpoint/CHANGELOG.md#102---2026-02-03)
* `https://github.com/aws/aws-advanced-go-wrapper/federated-auth`: [v1.0.5](federated-auth/CHANGELOG.md#105---2026-02-03)
* `https://github.com/aws/aws-advanced-go-wrapper/iam`: [v1.0.5](iam/CHANGELOG.md#105---2026-02-03)
* `https://github.com/aws/aws-advanced-go-wrapper/mysql-driver`: [v1.0.5](mysql-driver/CHANGELOG.md#105---2026-02-03)
* `https://github.com/aws/aws-advanced-go-wrapper/okta`: [v1.0.5](okta/CHANGELOG.md#105---2026-02-03)
* `https://github.com/aws/aws-advanced-go-wrapper/otlp`: [v1.0.5](otlp/CHANGELOG.md#105---2026-02-03)
* `https://github.com/aws/aws-advanced-go-wrapper/pgx-driver`: [v1.0.5](pgx-driver/CHANGELOG.md#105---2026-02-03)
* `https://github.com/aws/aws-advanced-go-wrapper/xray`: [v1.0.5](xray/CHANGELOG.md#105---2026-02-03)