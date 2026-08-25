# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### :warning: Action required before upgrading

The Custom Endpoint Plugin has not run since `awssql` v2 and is now functional, so `plugins=customEndpoint`
stops being a no-op. Grant `rds:DescribeDBClusterEndpoints` to the credentials the wrapper resolves before
upgrading, regardless of your database authentication method, or connections to a custom endpoint will fail.
See [UsingTheCustomEndpointPlugin.md](docs/user-guide/using-plugins/UsingTheCustomEndpointPlugin.md).

### :boom: Breaking Changes

* Custom endpoint membership is now enforced for failover and read/write splitting, so the hosts available
  for failover shrink to the endpoint's members. Excluding the writer leaves no writer to select. See
  [UsingTheCustomEndpointPlugin.md](docs/user-guide/using-plugins/UsingTheCustomEndpointPlugin.md) ([PR #583](https://github.com/aws/aws-advanced-go-wrapper/pull/583)).
* `GetUpdatedHostListWithTimeout` now returns a filtered topology. Use `ForceRefreshHostListWithTimeout`
  for the raw one ([PR #583](https://github.com/aws/aws-advanced-go-wrapper/pull/583)).
* The Custom Endpoint Plugin gates most network-bound methods, adding per-statement overhead when enabled
  ([PR #583](https://github.com/aws/aws-advanced-go-wrapper/pull/583)).

### :bug: Fixed

* Custom Endpoint Plugin was never invoked, so no host filtering was ever applied ([PR #583](https://github.com/aws/aws-advanced-go-wrapper/pull/583)).
* Custom endpoint monitoring used the ambient AWS region instead of the endpoint's, failing every
  connection when they differed ([PR #583](https://github.com/aws/aws-advanced-go-wrapper/pull/583)).
* Unbounded `DescribeDBClusterEndpoints` retry loop on any SDK error ([PR #583](https://github.com/aws/aws-advanced-go-wrapper/pull/583)).
* Panic on the monitor goroutine for an endpoint with no `CustomEndpointType`, which terminated the host
  process ([PR #583](https://github.com/aws/aws-advanced-go-wrapper/pull/583)).
* Monitor shutdown could block driver shutdown and the shared monitor-cleanup goroutine ([PR #583](https://github.com/aws/aws-advanced-go-wrapper/pull/583)).
* Host filtering lapsed periodically, and could be lost entirely when a monitor was recreated ([PR #583](https://github.com/aws/aws-advanced-go-wrapper/pull/583)).
* `readWriteSplitting` served reads from the writer while reporting success, when only one host was
  allowed and it was a reader ([PR #583](https://github.com/aws/aws-advanced-go-wrapper/pull/583)).
* Reader failover could spin at 100% of a core for the whole `failoverTimeoutMs` with no reader candidate
  ([PR #583](https://github.com/aws/aws-advanced-go-wrapper/pull/583)).
* Custom endpoint monitor logs named neither the endpoint nor the underlying error, and some rendered as
  `%!v(MISSING)` ([PR #583](https://github.com/aws/aws-advanced-go-wrapper/pull/583)).

### :crab: Changed

* Documented the required IAM permission and its correct scope, corrected `wrapperPlugins` to `plugins`,
  and added tests so the guide and the property definitions cannot drift apart. See [UsingTheCustomEndpointPlugin.md](docs/user-guide/using-plugins/UsingTheCustomEndpointPlugin.md) ([PR #583](https://github.com/aws/aws-advanced-go-wrapper/pull/583)).
* Added custom endpoint integration tests, which the suite had none of ([PR #583](https://github.com/aws/aws-advanced-go-wrapper/pull/583)).

### :warning: Known limitations

* A custom endpoint's *type* (`READER`, `WRITER`, `ANY`) is not enforced, only its member list.
* Writer selection during failover is not filtered by custom endpoint membership; reader selection is.

# Release (2026-07-29)
## General Highlights
### :bug: Fixed
* Coalesce concurrent MonitorManager initialization to prevent untracked duplicates monitors ([Issue #502](https://github.com/aws/aws-advanced-go-wrapper/issues/502)).
* Exclude caller cancellation and stale connection from IsNetworkError ([Issue #492](https://github.com/aws/aws-advanced-go-wrapper/issues/492)).
* Minor regex fixes for mask-helper ([PR #494](https://github.com/aws/aws-advanced-go-wrapper/pull/494)).
* Pooled connections created with stale credentials ([PR #452](https://github.com/aws/aws-advanced-go-wrapper/pull/452)).

### :crab: Changed
* Refactor PG SQL queries to be fully qualified ([PR #523](https://github.com/aws/aws-advanced-go-wrapper/pull/523)).
* Update known limitations documentation - GDB failover is supported ([PR #505](https://github.com/aws/aws-advanced-go-wrapper/pull/505)).

## Module Highlights
* `https://github.com/aws/aws-advanced-go-wrapper/auth-helpers`: [v1.1.3](auth-helpers/CHANGELOG.md#113---2026-07-29)
* `https://github.com/aws/aws-advanced-go-wrapper/aws-secrets-manager`: [v1.1.4](aws-secrets-manager/CHANGELOG.md#114---2026-07-29)
* `https://github.com/aws/aws-advanced-go-wrapper/awssql/v2`: [v2.0.3](awssql/CHANGELOG.md#203---2026-07-29)
* `https://github.com/aws/aws-advanced-go-wrapper/custom-endpoint`: [v1.0.6](custom-endpoint/CHANGELOG.md#106---2026-07-29)
* `https://github.com/aws/aws-advanced-go-wrapper/federated-auth`: [v1.1.3](federated-auth/CHANGELOG.md#113---2026-07-29)
* `https://github.com/aws/aws-advanced-go-wrapper/iam`: [v1.1.3](iam/CHANGELOG.md#113---2026-07-29)
* `https://github.com/aws/aws-advanced-go-wrapper/mysql-driver`: [v1.1.3](mysql-driver/CHANGELOG.md#113---2026-07-29)
* `https://github.com/aws/aws-advanced-go-wrapper/okta`: [v1.1.3](okta/CHANGELOG.md#113---2026-07-29)
* `https://github.com/aws/aws-advanced-go-wrapper/otlp`: [v1.0.9](otlp/CHANGELOG.md#109---2026-07-29)
* `https://github.com/aws/aws-advanced-go-wrapper/pgx-driver`: [v1.1.3](pgx-driver/CHANGELOG.md#113---2026-07-29)
* `https://github.com/aws/aws-advanced-go-wrapper/xray`: [v1.0.9](xray/CHANGELOG.md#109---2026-07-29)

# Release (2026-07-02)
## General Highlights
### :bug: Fixed
* Thread-safe plugin and driver registration ([PR #459](https://github.com/aws/aws-advanced-go-wrapper/pull/459)).
* Proper mutex synchronization across various components and thread-safe map access ([PR #462](https://github.com/aws/aws-advanced-go-wrapper/pull/462)).

### :crab: Changed
* Driver method have descriptors/flags that help define execution behavior ([PR #463](https://github.com/aws/aws-advanced-go-wrapper/pull/463)).
* Various performance optimizations. To learn more, see ([PR #471](https://github.com/aws/aws-advanced-go-wrapper/pull/471)).

## Module Highlights
* `https://github.com/aws/aws-advanced-go-wrapper/auth-helpers`: [v1.1.2](auth-helpers/CHANGELOG.md#112---2026-07-02)
* `https://github.com/aws/aws-advanced-go-wrapper/aws-secrets-manager`: [v1.1.3](aws-secrets-manager/CHANGELOG.md#113---2026-07-02)
* `https://github.com/aws/aws-advanced-go-wrapper/awssql/v2`: [v2.0.2](awssql/CHANGELOG.md#202---2026-07-02)
* `https://github.com/aws/aws-advanced-go-wrapper/custom-endpoint`: [v1.0.5](custom-endpoint/CHANGELOG.md#105---2026-07-02)
* `https://github.com/aws/aws-advanced-go-wrapper/federated-auth`: [v1.1.2](federated-auth/CHANGELOG.md#112---2026-07-02)
* `https://github.com/aws/aws-advanced-go-wrapper/iam`: [v1.1.2](iam/CHANGELOG.md#112---2026-07-02)
* `https://github.com/aws/aws-advanced-go-wrapper/mysql-driver`: [v1.1.2](mysql-driver/CHANGELOG.md#112---2026-07-02)
* `https://github.com/aws/aws-advanced-go-wrapper/okta`: [v1.1.2](okta/CHANGELOG.md#112---2026-07-02)
* `https://github.com/aws/aws-advanced-go-wrapper/otlp`: [v1.0.8](otlp/CHANGELOG.md#108---2026-07-02)
* `https://github.com/aws/aws-advanced-go-wrapper/pgx-driver`: [v1.1.2](pgx-driver/CHANGELOG.md#112---2026-07-02)
* `https://github.com/aws/aws-advanced-go-wrapper/xray`: [v1.0.8](xray/CHANGELOG.md#108---2026-07-02)

# Release (2026-05-26)
## General Highlights
### :crab: Changed
* Refactor PG SQL queries to be fully qualified ([Commit #a07683f](https://github.com/aws/aws-advanced-go-wrapper/commit/a07683f09f69e972a460640e5cc3845de7a97489)).

### :bug: Fixed
* Remove registered dialects check so that custom driver dialects will not be blocked ([PR #431](https://github.com/aws/aws-advanced-go-wrapper/pull/431)).

## Module Highlights
* `https://github.com/aws/aws-advanced-go-wrapper/auth-helpers`: [v1.1.1](auth-helpers/CHANGELOG.md#111---2026-05-26)
* `https://github.com/aws/aws-advanced-go-wrapper/aws-secrets-manager`: [v1.1.2](aws-secrets-manager/CHANGELOG.md#112---2026-05-26)
* `https://github.com/aws/aws-advanced-go-wrapper/awssql/v2`: [v2.0.1](awssql/CHANGELOG.md#201---2026-05-26)
* `https://github.com/aws/aws-advanced-go-wrapper/custom-endpoint`: [v1.0.4](custom-endpoint/CHANGELOG.md#104---2026-05-26)
* `https://github.com/aws/aws-advanced-go-wrapper/federated-auth`: [v1.1.1](federated-auth/CHANGELOG.md#111---2026-05-26)
* `https://github.com/aws/aws-advanced-go-wrapper/iam`: [v1.1.1](iam/CHANGELOG.md#111---2026-05-26)
* `https://github.com/aws/aws-advanced-go-wrapper/mysql-driver`: [v1.1.1](mysql-driver/CHANGELOG.md#111---2026-05-26)
* `https://github.com/aws/aws-advanced-go-wrapper/okta`: [v1.1.1](okta/CHANGELOG.md#111---2026-05-26)
* `https://github.com/aws/aws-advanced-go-wrapper/otlp`: [v1.0.7](otlp/CHANGELOG.md#107---2026-05-26)
* `https://github.com/aws/aws-advanced-go-wrapper/pgx-driver`: [v1.1.1](pgx-driver/CHANGELOG.md#111---2026-05-26)
* `https://github.com/aws/aws-advanced-go-wrapper/xray`: [v1.0.7](xray/CHANGELOG.md#107---2026-05-26)

# Release (2026-04-06)
## General Highlights
### :boom: Breaking Changes

> [!WARNING]\
> This release removes the suggested ClusterId functionality ([PR #355](https://github.com/aws/aws-advanced-go-wrapper/pull/355)).
> #### Suggested ClusterId Functionality
> Prior to this change, the wrapper would generate a unique cluster ID based on the connection string and the cluster topology; however, in some cases (such as custom endpoints, IP addresses, and CNAME aliases, etc), the wrapper would generate an incorrect identifier. This change was needed to prevent applications with several clusters from accidentally relying on incorrect topology during failover which could result in the wrapper failing to complete failover successfully.
> #### Migration
> | Number of Database Clusters in Use | Requires Changes | Action Items                                                                                                                                                                                                                                                                                                                                                                            |
> |------------------------------------|------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
> | Single database cluster            | No               | No changes required                                                                                                                                                                                                                                                                                                                                                                     |
> | Multiple database clusters         | Yes              | Review all connection strings and add mandatory `clusterId` parameter. See [Cluster ID documentation](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/ClusterId.md) for configuration details |

### :magic_wand: Added
* Global Database (GDB) Support, including:
  * Aurora GDB Host List Provider Infrastructure ([PR #355](https://github.com/aws/aws-advanced-go-wrapper/pull/355)). For more information, see the [Global Databases documentation](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/GlobalDatabases.md).
  * GDB Failover Plugin ([PR #381](https://github.com/aws/aws-advanced-go-wrapper/pull/381)). For more information, see the [documentation](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/using-plugins/UsingTheGdbFailoverPlugin.md).
  * GDB Auth Support ([PR #398](https://github.com/aws/aws-advanced-go-wrapper/pull/398)):
    * [IAM Authentication Plugin](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/using-plugins/UsingTheIamAuthenticationPlugin.md#using-iam-authentication-with-global-databases)
    * [Okta Authentication Plugin](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/using-plugins/UsingTheOktaAuthPlugin.md#using-okta-authentication-with-global-databases)
    * [Federated Authentication Plugin](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/using-plugins/UsingTheFederatedAuthPlugin.md#using-federated-authentication-with-global-databases)
  * GDB Read/Write Splitting Plugin ([PR #401](https://github.com/aws/aws-advanced-go-wrapper/pull/401)). For more information, see the [documentation](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/using-plugins/UsingTheGdbReadWriteSplittingPlugin.md).
* Failover Plugin: `clusterTopologyConnectTimeoutMs` and `clusterTopologySocketTimeoutMs` connection parameters for configuring topology query timeouts ([PR #381](https://github.com/aws/aws-advanced-go-wrapper/pull/381)). For more information, see the [Failover Plugin documentation](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/using-plugins/UsingTheFailoverPlugin.md).

### :bug: Fixed
* Wrong host ID in host info ([PR #333](https://github.com/aws/aws-advanced-go-wrapper/pull/333)).
* Do not consider XX000 errors as network-related errors ([PR #334](https://github.com/aws/aws-advanced-go-wrapper/pull/334)).
* Minor blue/green fixes ([PR #343](https://github.com/aws/aws-advanced-go-wrapper/pull/343)).
* Fix issue with blue/green metadata for PG databases ([PR #348](https://github.com/aws/aws-advanced-go-wrapper/pull/348)).
* Read messages from embedded FS ([PR #409](https://github.com/aws/aws-advanced-go-wrapper/pull/409)).

### :crab: Changed
* Remove IP Address checking in staleDnsHelper ([PR #363](https://github.com/aws/aws-advanced-go-wrapper/pull/363)).
* Read/Write Splitting no longer uses a query to toggle between read and read/write mode ([PR #407](https://github.com/aws/aws-advanced-go-wrapper/pull/407)).

## Module Highlights
* `https://github.com/aws/aws-advanced-go-wrapper/auth-helpers`: [v1.1.0](auth-helpers/CHANGELOG.md#110---2026-04-06)
* `https://github.com/aws/aws-advanced-go-wrapper/aws-secrets-manager`: [v1.1.1](aws-secrets-manager/CHANGELOG.md#111---2026-04-06)
* `https://github.com/aws/aws-advanced-go-wrapper/awssql/v2`: [v2.0.0](awssql/CHANGELOG.md#200---2026-04-06)
* `https://github.com/aws/aws-advanced-go-wrapper/custom-endpoint`: [v1.0.3](custom-endpoint/CHANGELOG.md#103---2026-04-06)
* `https://github.com/aws/aws-advanced-go-wrapper/federated-auth`: [v1.1.0](federated-auth/CHANGELOG.md#110---2026-04-06)
* `https://github.com/aws/aws-advanced-go-wrapper/iam`: [v1.1.0](iam/CHANGELOG.md#110---2026-04-06)
* `https://github.com/aws/aws-advanced-go-wrapper/mysql-driver`: [v1.1.0](mysql-driver/CHANGELOG.md#110---2026-04-06)
* `https://github.com/aws/aws-advanced-go-wrapper/okta`: [v1.1.0](okta/CHANGELOG.md#110---2026-04-06)
* `https://github.com/aws/aws-advanced-go-wrapper/otlp`: [v1.0.6](otlp/CHANGELOG.md#106---2026-04-06)
* `https://github.com/aws/aws-advanced-go-wrapper/pgx-driver`: [v1.1.0](pgx-driver/CHANGELOG.md#110---2026-04-06)
* `https://github.com/aws/aws-advanced-go-wrapper/xray`: [v1.0.6](xray/CHANGELOG.md#106---2026-04-06)

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
