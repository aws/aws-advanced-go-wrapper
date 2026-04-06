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
* Aurora Connection Tracker Plugin ([PR #272](https://github.com/aws/aws-advanced-go-wrapper/pull/272)). For more information, see the [documentation](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/UsingTheAuroraConnectionTrackerPlugin.md).
* Developer Plugin ([PR #274](https://github.com/aws/aws-advanced-go-wrapper/pull/274)). For more information, see the [documentation](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/using-plugins/UsingTheDeveloperPlugin.md).

### :bug: Fixed
* Blue Green Plugin Status Monitor to poll with the correct rate ([PR #279](https://github.com/aws/aws-advanced-go-wrapper/pull/279)).

## [1.3.0] - 2025-12-16
### :magic_wand: Added
* Aurora Initial Connection Strategy Plugin ([PR #282](https://github.com/aws/aws-advanced-go-wrapper/pull/282)). For more information, see the [documentation](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/using-plugins/UsingTheAuroraInitialConnectionStrategyPlugin.md).

### :crab: Changed
* Update various dependencies

## [1.4.0] - 2026-02-03
### :magic_wand: Added
* Support for EU, AU, and UK domains ([PR #325](https://github.com/aws/aws-advanced-go-wrapper/pull/325)).

### :bug: Fixed
* Embed messages file to allow it to be bundled in with binary ([Issue #301](https://github.com/aws/aws-advanced-go-wrapper/issues/301)).
* During B/G switchover, ensure IAM host name should be based on green host ([PR #321](https://github.com/aws/aws-advanced-go-wrapper/pull/321)).
* Address race conditions associated with PluginServiceImpl by implementing a separate PartialPluginService to be used by monitoring structs for plugins such as BlueGreen, CustomEndpoint, and Limitless ([Issue #318](https://github.com/aws/aws-advanced-go-wrapper/issues/318)).
* Stop B/G monitors after switchover completes ([PR #323](https://github.com/aws/aws-advanced-go-wrapper/pull/323)).
* Goroutine to clean up open connections when failover never happens ([PR #327](https://github.com/aws/aws-advanced-go-wrapper/pull/327)).
* Ensure B/G monitors are set up ([PR #330](https://github.com/aws/aws-advanced-go-wrapper/pull/330)).

### :crab: Changed
* Cache efm2 monitor key for better performance ([PR #328](https://github.com/aws/aws-advanced-go-wrapper/pull/328)).

## [2.0.0] - 2026-04-06
### :boom: Breaking Changes

> [!WARNING]\
> 2.0 removes the suggested ClusterId functionality ([PR #355](https://github.com/aws/aws-advanced-go-wrapper/pull/355)).
> #### Suggested ClusterId Functionality
> Prior to this change, the wrapper would generate a unique cluster ID based on the connection string and the cluster topology; however, in some cases (such as custom endpoints, IP addresses, and CNAME aliases, etc), the wrapper would generate an incorrect identifier. Removing the suggested cluster id functionality was needed to prevent applications with several clusters from relying on incorrect topology during failover and possibly failing to complete failover successfully.
> #### Migration
> | Number of Database Clusters in Use | Requires Changes | Action Items                                                                                                                                                                                                                                                                                                                                                                            |
> |------------------------------------|------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
> | Single database cluster            | No               | No changes required                                                                                                                                                                                                                                                                                                                                                                     |
> | Multiple database clusters         | Yes              | Review all connection strings and add mandatory `clusterId` parameter. See [Cluster ID documentation](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/ClusterId.md) for configuration details |

> [!WARNING]\
> 2.0.0 changes the `ConnectionPluginFactory.GetInstance` method signature.
> #### ConnectionPluginFactory.GetInstance Signature Change
> The `ConnectionPluginFactory.GetInstance(PluginService, *RWMap)` method has been replaced with `ConnectionPluginFactory.GetInstance(ServicesContainer, *RWMap)`.
> The `ServicesContainer` provides access to the `PluginService` via `servicesContainer.GetPluginService()`, as well as other services.
> If you have created custom `ConnectionPluginFactory` implementations, update them as shown below.
> #### Migration
> ```go
> // Before
> func (f MyPluginFactory) GetInstance(pluginService driver_infrastructure.PluginService, props *utils.RWMap[string, string]) (driver_infrastructure.ConnectionPlugin, error) {
>     return NewMyPlugin(pluginService, props)
> }
>
> // After
> func (f MyPluginFactory) GetInstance(servicesContainer driver_infrastructure.ServicesContainer, props *utils.RWMap[string, string]) (driver_infrastructure.ConnectionPlugin, error) {
>     return NewMyPlugin(servicesContainer.GetPluginService(), props)
> }
> ```

### :magic_wand: Added
* Global Database (GDB) Support, including:
  * Aurora GDB Host List Provider Infrastructure ([PR #355](https://github.com/aws/aws-advanced-go-wrapper/pull/355)). For more information, see the [Global Databases documentation](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/GlobalDatabases.md).
  * GDB Failover Plugin ([PR #381](https://github.com/aws/aws-advanced-go-wrapper/pull/381)). For more information, see the [documentation](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/using-plugins/UsingTheGdbFailoverPlugin.md).
  * GDB Auth Support ([PR #398](https://github.com/aws/aws-advanced-go-wrapper/pull/398)):
    * [IAM Authentication Plugin](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/using-plugins/UsingTheIamAuthenticationPlugin.md#using-iam-authentication-with-global-databases)
    * [Okta Authentication Plugin](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/using-plugins/UsingTheOktaAuthPlugin.md#using-okta-authentication-with-global-databases)
    * [Federated Authentication Plugin](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/using-plugins/UsingTheFederatedAuthPlugin.md#using-federated-authentication-with-global-databases)
  * GDB Read/Write Splitting Plugin ([PR #401](https://github.com/aws/aws-advanced-go-wrapper/pull/401)). For more information, see the [documentation](https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/using-plugins/UsingTheGdbReadWriteSplittingPlugin.md).

### :bug: Fixed
* Wrong host ID in host info ([PR #333](https://github.com/aws/aws-advanced-go-wrapper/pull/333)).
* Do not consider XX000 errors as network-related errors ([PR #334](https://github.com/aws/aws-advanced-go-wrapper/pull/334)).
* Minor blue/green fixes ([PR #343](https://github.com/aws/aws-advanced-go-wrapper/pull/343)).
* Fix issue with blue/green metadata for PG databases ([PR #348](https://github.com/aws/aws-advanced-go-wrapper/pull/348)).
* Read messages from embedded FS ([PR #409](https://github.com/aws/aws-advanced-go-wrapper/pull/409)).

### :crab: Changed
* Remove IP Address checking in staleDnsHelper ([PR #363](https://github.com/aws/aws-advanced-go-wrapper/pull/363)).
* Read/Write Splitting no longer uses a query to toggle between read and read/write mode ([PR #407](https://github.com/aws/aws-advanced-go-wrapper/pull/407)).

[1.0.0]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/awssql%2Fv1.0.0
[1.1.0]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/awssql%2Fv1.1.0
[1.1.1]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/awssql%2Fv1.1.1
[1.2.0]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/awssql%2Fv1.2.0
[1.3.0]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/awssql%2Fv1.3.0
[1.4.0]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/awssql%2Fv1.4.0
[2.0.0]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/awssql%2Fv2.0.0
