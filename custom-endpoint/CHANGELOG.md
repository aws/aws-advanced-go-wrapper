# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/#semantic-versioning-200).

## [Unreleased]

> Ships as `1.1.0`: the plugin goes from inert to functional. Requires `awssql/v2` >= 2.1.0.

### :warning: Action required before upgrading

Grant `rds:DescribeDBClusterEndpoints` to the credentials the wrapper resolves, regardless of your database
authentication method. Without it, connections and statements to a custom endpoint fail after
`waitForCustomEndpointInfoTimeoutMs` (default 5000 ms). Set `waitForCustomEndpointInfo=false`, or remove
`customEndpoint` from `plugins`, to keep operating without it. See [UsingTheCustomEndpointPlugin.md](../docs/user-guide/using-plugins/UsingTheCustomEndpointPlugin.md).

### :boom: Breaking Changes

* Custom endpoint membership is now enforced for failover and read/write splitting. Excluding the writer
  leaves no writer to select, so reader failover and switching out of read-only mode will return an error.
  See [UsingTheCustomEndpointPlugin.md](../docs/user-guide/using-plugins/UsingTheCustomEndpointPlugin.md) ([PR #583](https://github.com/aws/aws-advanced-go-wrapper/pull/583)).
* The plugin gates most network-bound methods, adding per-statement overhead. `Tx.Rollback`,
  `Conn.IsValid` and `Conn.ResetSession` are not gated ([PR #583](https://github.com/aws/aws-advanced-go-wrapper/pull/583)).

### :bug: Fixed

* The plugin never entered the connect pipeline, so no monitor ran and no host filtering was applied ([PR #583](https://github.com/aws/aws-advanced-go-wrapper/pull/583)).
* The monitor used the ambient AWS region instead of the endpoint's, returning zero endpoints and failing
  every connection when they differed ([PR #583](https://github.com/aws/aws-advanced-go-wrapper/pull/583)).
* Unbounded `DescribeDBClusterEndpoints` retry loop on any SDK error, including from a non-positive
  `customEndpointInfoRefreshRateMs` ([PR #583](https://github.com/aws/aws-advanced-go-wrapper/pull/583)).
* Panic on the monitor goroutine for an endpoint with no `CustomEndpointType` ([PR #583](https://github.com/aws/aws-advanced-go-wrapper/pull/583)).
* `Stop` could block driver shutdown and the shared monitor-cleanup goroutine ([PR #583](https://github.com/aws/aws-advanced-go-wrapper/pull/583)).
* Host filtering lapsed periodically, and a monitor being recreated could discard its replacement's data
  ([PR #583](https://github.com/aws/aws-advanced-go-wrapper/pull/583)).
* Two `sql.DB` handles differing only by port shared one endpoint info cache entry ([PR #583](https://github.com/aws/aws-advanced-go-wrapper/pull/583)).
* A missing IAM permission now logs at `ERROR` naming the permission and pauses monitoring for 5 minutes,
  instead of retrying silently ([PR #583](https://github.com/aws/aws-advanced-go-wrapper/pull/583)).
* Monitor logs named neither the endpoint nor the underlying error, and some rendered as
  `%!v(MISSING)` ([PR #583](https://github.com/aws/aws-advanced-go-wrapper/pull/583)).

### :crab: Changed

* Added integration tests covering membership filtering, member-list changes and failover through a real
  custom endpoint ([PR #583](https://github.com/aws/aws-advanced-go-wrapper/pull/583)).

## [1.0.6] - 2026-07-29
### :crab: Changed
* Update dependency `github.com/aws/aws-advanced-go-wrapper/awssql` to v2.0.3
* Update dependency `github.com/aws/aws-advanced-go-wrapper/auth-helpers` to v1.1.3

## [1.0.5] - 2026-07-02
### :crab: Changed
* Update dependency `github.com/aws/aws-advanced-go-wrapper/awssql` to v2.0.2
* Update dependency `github.com/aws/aws-advanced-go-wrapper/auth-helpers` to v1.1.2

## [1.0.4] - 2026-05-26
### :crab: Changed
* Update dependency `github.com/aws/aws-advanced-go-wrapper/awssql` to v2.0.1
* Update dependency `github.com/aws/aws-advanced-go-wrapper/auth-helpers` to v1.1.1

## [1.0.3] - 2026-04-06
### :crab: Changed
* Update dependency `github.com/aws/aws-advanced-go-wrapper/awssql` to v2.0.0
* Update dependency `github.com/aws/aws-advanced-go-wrapper/auth-helpers` to v1.1.0

## [1.0.2] - 2026-02-03
### :bug: Fixed
* Address race conditions associated with PluginServiceImpl by implementing a separate PartialPluginService to be used by monitoring structs for plugins such as BlueGreen, CustomEndpoint, and Limitless ([Issue #318](https://github.com/aws/aws-advanced-go-wrapper/issues/318)).

### :crab: Changed
* Update dependency `github.com/aws/aws-advanced-go-wrapper/awssql` to v1.5.0
* Update dependency `github.com/aws/aws-advanced-go-wrapper/auth-helpers` to v1.0.5

## [1.0.1] - 2025-12-16
### :crab: Changed
* Update dependency `github.com/aws/aws-advanced-go-wrapper/awssql` to v1.3.0
* Update dependency `github.com/aws/aws-advanced-go-wrapper/auth-helpers` to v1.0.4

## [1.0.0] - 2025-12-04
* The Custom Endpoint Plugin adds support for RDS custom endpoints. To see information on how to configure and use the Custom Endpoint Plugin, see [Using the Custom Endpoint Plugin](../docs/user-guide/using-plugins/UsingTheCustomEndpointPlugin.md).

[1.0.6]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/custom-endpoint%2Fv1.0.6
[1.0.5]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/custom-endpoint%2Fv1.0.5
[1.0.4]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/custom-endpoint%2Fv1.0.4
[1.0.3]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/custom-endpoint%2Fv1.0.3
[1.0.2]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/custom-endpoint%2Fv1.0.2
[1.0.1]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/custom-endpoint%2Fv1.0.1
[1.0.0]: https://github.com/aws/aws-advanced-go-wrapper/releases/tag/custom-endpoint%2Fv1.0.0
