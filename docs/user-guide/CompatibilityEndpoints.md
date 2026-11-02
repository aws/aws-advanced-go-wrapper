# Database URL Types Compatibility

Many different URL types (endpoints) can be used with the AWS Advanced Go Wrapper, but certain URL types are not compatible with certain plugins. Several plugins decide what to do by recognizing the shape of the host in your DSN: an Aurora cluster endpoint carries its role in its name, while an IP address or a CNAME pointing at that same endpoint does not. This page lists the URL types and which plugins work with each.

- [Aurora Global Database Endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-global-database-connecting.html) - `<global-db-name>.global-<XYZ>.global.rds.amazonaws.com`
- [Aurora Cluster Writer Endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Endpoints.Cluster.html) - `<cluster-name>.cluster-<XYZ>.<region>.rds.amazonaws.com`
- [Aurora Cluster Reader Endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Endpoints.Reader.html) - `<cluster-name>.cluster-ro-<XYZ>.<region>.rds.amazonaws.com`
- [Aurora Cluster Custom Endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Endpoints.Custom.html) - `<custom-endpoint-name>.cluster-custom-<XYZ>.<region>.rds.amazonaws.com`
- [Aurora Instance Endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Endpoints.Instance.html) - `<instance-name>.<XYZ>.<region>.rds.amazonaws.com`
- [RDS Multi-AZ DB Cluster Writer Endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts-connection-management.html) - `<cluster-name>.cluster-<XYZ>.<region>.rds.amazonaws.com`
- [RDS Multi-AZ DB Cluster Reader Endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts-connection-management.html) - `<cluster-name>.cluster-ro-<XYZ>.<region>.rds.amazonaws.com`
- [RDS Instance Endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts-connection-management.html) - `<instance-name>.<XYZ>.<region>.rds.amazonaws.com`
- [RDS Proxy Endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/rds-proxy-endpoints.html) - `<proxy-name>.proxy-<XYZ>.<region>.rds.amazonaws.com`
- [DB Shard Group Endpoint (Aurora Limitless)](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/limitless-shard.html) - `<shard-group-name>.shardgrp-<XYZ>.<region>.rds.amazonaws.com`
- [IP address](https://en.wikipedia.org/wiki/IP_address) - IPv4 or IPv6 addresses, for example: `8.8.0.0`, `2001:0db8:85a3:0000:0000:8a2e:0370:7334`
- User custom domain (CNAME alias) - any other domain names, for example: `my-database.my-domain.com`

The last two rows of each table are not plugins. They are behaviors that several plugins depend on, listed here because they fail on the same endpoint types.

### Legend

| Symbol | Meaning |
|:------:|---------|
| ✅ | Supported |
| ❌ | Not supported |

A cell with a `[^N]` footnote marker links to the configuration required or the reason it fails on that endpoint type.

## Aurora Global Database Endpoint

| Plugin codes / Database URL types | Aurora Global Database Endpoint |
|-----------------------------------|:-------------------------------:|
| [`customEndpoint`](./using-plugins/UsingTheCustomEndpointPlugin.md) | ❌ [^1] |
| [`efm`](./using-plugins/UsingTheHostMonitoringPlugin.md) | ✅ |
| [`failover`](./using-plugins/UsingTheFailoverPlugin.md) | ✅ |
| [`gdbFailover`](./using-plugins/UsingTheGdbFailoverPlugin.md) | ✅ |
| [`iam`](./using-plugins/UsingTheIamAuthenticationPlugin.md) / [`awsSecretsManager`](./using-plugins/UsingTheAwsSecretsManagerPlugin.md) / [`federatedAuth`](./using-plugins/UsingTheFederatedAuthPlugin.md) / [`okta`](./using-plugins/UsingTheOktaAuthPlugin.md) | ✅ [^2] |
| [`readWriteSplitting`](./using-plugins/UsingTheReadWriteSplittingPlugin.md) | ✅ |
| [`gdbReadWriteSplitting`](./using-plugins/UsingTheGdbReadWriteSplittingPlugin.md) | ✅ |
| [`auroraConnectionTracker`](./UsingTheAuroraConnectionTrackerPlugin.md) | ✅ |
| [`initialConnection`](./using-plugins/UsingTheAuroraInitialConnectionStrategyPlugin.md) role verification | ✅ |
| [`limitless`](./using-plugins/UsingTheLimitlessConnectionPlugin.md) | ❌ [^3] |
| [`bg`](./using-plugins/UsingTheBlueGreenPlugin.md) | ❌ [^4] |
| `connectTime` / `executionTime` / `dev` | ✅ [^5] |
| Stale DNS correction, within `failover` | ✅ |
| [Internal connection pooling](./using-plugins/UsingTheReadWriteSplittingPlugin.md#internal-connection-pooling) | ✅ |

## Aurora Endpoints

| Plugin codes / Database URL types | Aurora Cluster Writer Endpoint | Aurora Cluster Reader Endpoint | Aurora Cluster Custom Endpoint | Aurora Instance Endpoint,<br>RDS Instance Endpoint |
|-----------------------------------|:------------------------------:|:------------------------------:|:------------------------------:|:--------------------------------------------------:|
| [`customEndpoint`](./using-plugins/UsingTheCustomEndpointPlugin.md) | ❌ [^1] | ❌ [^1] | ✅ | ❌ [^1] |
| [`efm`](./using-plugins/UsingTheHostMonitoringPlugin.md) | ✅ | ✅ | ✅ | ✅ |
| [`failover`](./using-plugins/UsingTheFailoverPlugin.md) | ✅ | ✅ | ✅ | ✅ |
| [`gdbFailover`](./using-plugins/UsingTheGdbFailoverPlugin.md) | ✅ | ✅ | ✅ | ✅ |
| [`iam`](./using-plugins/UsingTheIamAuthenticationPlugin.md) / [`awsSecretsManager`](./using-plugins/UsingTheAwsSecretsManagerPlugin.md) / [`federatedAuth`](./using-plugins/UsingTheFederatedAuthPlugin.md) / [`okta`](./using-plugins/UsingTheOktaAuthPlugin.md) | ✅ [^2] | ✅ [^2] | ✅ [^2] | ✅ |
| [`readWriteSplitting`](./using-plugins/UsingTheReadWriteSplittingPlugin.md) | ✅ | ✅ | ✅ | ✅ |
| [`gdbReadWriteSplitting`](./using-plugins/UsingTheGdbReadWriteSplittingPlugin.md) | ✅ | ✅ | ✅ | ✅ |
| [`auroraConnectionTracker`](./UsingTheAuroraConnectionTrackerPlugin.md) | ✅ | ✅ | ✅ | ✅ |
| [`initialConnection`](./using-plugins/UsingTheAuroraInitialConnectionStrategyPlugin.md) role verification | ✅ | ✅ | ✅ [^6] | ❌ [^7] |
| [`limitless`](./using-plugins/UsingTheLimitlessConnectionPlugin.md) | ❌ [^3] | ❌ [^3] | ❌ [^3] | ❌ [^3] |
| [`bg`](./using-plugins/UsingTheBlueGreenPlugin.md) | ✅ | ✅ | ✅ | ✅ |
| `connectTime` / `executionTime` / `dev` | ✅ [^5] | ✅ [^5] | ✅ [^5] | ✅ [^5] |
| Stale DNS correction, within `failover` | ✅ | ❌ [^8] | ❌ [^8] | ❌ [^8] |
| [Internal connection pooling](./using-plugins/UsingTheReadWriteSplittingPlugin.md#internal-connection-pooling) | ✅ | ✅ | ✅ | ✅ |

## RDS Multi-AZ, RDS Proxy and Limitless Endpoints

| Plugin codes / Database URL types | RDS Multi-AZ DB Cluster Writer Endpoint | RDS Multi-AZ DB Cluster Reader Endpoint | RDS Proxy Endpoint | DB Shard Group Endpoint (Aurora Limitless) |
|-----------------------------------|:---------------------------------------:|:---------------------------------------:|:------------------:|:------------------------------------------:|
| [`customEndpoint`](./using-plugins/UsingTheCustomEndpointPlugin.md) | ❌ [^1] | ❌ [^1] | ❌ [^1] | ❌ [^1] |
| [`efm`](./using-plugins/UsingTheHostMonitoringPlugin.md) | ✅ | ✅ | ❌ [^9] | ❌ [^9] |
| [`failover`](./using-plugins/UsingTheFailoverPlugin.md) | ✅ | ✅ | ❌ [^10] | ❌ [^11] |
| [`gdbFailover`](./using-plugins/UsingTheGdbFailoverPlugin.md) | ✅ | ✅ | ❌ [^10] | ❌ [^11] |
| [`iam`](./using-plugins/UsingTheIamAuthenticationPlugin.md) / [`awsSecretsManager`](./using-plugins/UsingTheAwsSecretsManagerPlugin.md) / [`federatedAuth`](./using-plugins/UsingTheFederatedAuthPlugin.md) / [`okta`](./using-plugins/UsingTheOktaAuthPlugin.md) | ✅ [^2] | ✅ [^2] | ✅ | ✅ |
| [`readWriteSplitting`](./using-plugins/UsingTheReadWriteSplittingPlugin.md) | ✅ | ✅ | ❌ [^12] | ❌ [^11] |
| [`gdbReadWriteSplitting`](./using-plugins/UsingTheGdbReadWriteSplittingPlugin.md) | ✅ | ✅ | ❌ [^12] | ❌ [^11] |
| [`auroraConnectionTracker`](./UsingTheAuroraConnectionTrackerPlugin.md) | ✅ | ✅ | ✅ | ✅ [^22] |
| [`initialConnection`](./using-plugins/UsingTheAuroraInitialConnectionStrategyPlugin.md) role verification | ✅ | ✅ | ❌ [^7] | ❌ [^7] |
| [`limitless`](./using-plugins/UsingTheLimitlessConnectionPlugin.md) | ❌ [^3] | ❌ [^3] | ❌ [^3] | ✅ |
| [`bg`](./using-plugins/UsingTheBlueGreenPlugin.md) | ❌ [^13] | ❌ [^13] | ✅ | ❌ [^14] |
| `connectTime` / `executionTime` / `dev` | ✅ [^5] | ✅ [^5] | ✅ [^5] | ✅ [^5] |
| Stale DNS correction, within `failover` | ✅ | ❌ [^8] | ❌ [^8] | ❌ [^8] |
| [Internal connection pooling](./using-plugins/UsingTheReadWriteSplittingPlugin.md#internal-connection-pooling) | ✅ | ✅ | ✅ | ✅ |

## IP Address and User Custom Domain

| Plugin codes / Database URL types | IP address | User custom domain (CNAME alias) |
|-----------------------------------|:----------:|:--------------------------------:|
| [`customEndpoint`](./using-plugins/UsingTheCustomEndpointPlugin.md) | ❌ [^1] | ❌ [^1] |
| [`efm`](./using-plugins/UsingTheHostMonitoringPlugin.md) | ❌ [^15] | ❌ [^15] |
| [`failover`](./using-plugins/UsingTheFailoverPlugin.md) | ✅ [^16] | ✅ [^16] |
| [`gdbFailover`](./using-plugins/UsingTheGdbFailoverPlugin.md) | ✅ [^16] | ✅ [^16] |
| [`iam`](./using-plugins/UsingTheIamAuthenticationPlugin.md) / [`awsSecretsManager`](./using-plugins/UsingTheAwsSecretsManagerPlugin.md) / [`federatedAuth`](./using-plugins/UsingTheFederatedAuthPlugin.md) / [`okta`](./using-plugins/UsingTheOktaAuthPlugin.md) | ✅ [^17] | ✅ [^17] |
| [`readWriteSplitting`](./using-plugins/UsingTheReadWriteSplittingPlugin.md) | ✅ [^16] | ✅ [^16] |
| [`gdbReadWriteSplitting`](./using-plugins/UsingTheGdbReadWriteSplittingPlugin.md) | ✅ [^16] | ✅ [^16] |
| [`auroraConnectionTracker`](./UsingTheAuroraConnectionTrackerPlugin.md) | ✅ | ✅ |
| [`initialConnection`](./using-plugins/UsingTheAuroraInitialConnectionStrategyPlugin.md) role verification | ✅ [^18] | ✅ [^18] |
| [`limitless`](./using-plugins/UsingTheLimitlessConnectionPlugin.md) | ❌ [^3] | ❌ [^3] |
| [`bg`](./using-plugins/UsingTheBlueGreenPlugin.md) | ❌ [^19] | ❌ [^19] |
| `connectTime` / `executionTime` / `dev` | ✅ [^5] | ✅ [^5] |
| Stale DNS correction, within `failover` | ❌ [^20] | ❌ [^20] |
| [Internal connection pooling](./using-plugins/UsingTheReadWriteSplittingPlugin.md#internal-connection-pooling) | ❌ [^21] | ❌ [^21] |

## Connecting Through a Custom Domain

A CNAME in front of a cluster endpoint is supported for failover. Set [`clusterInstanceHostPattern`](./using-plugins/UsingTheFailoverPlugin.md#failover-parameters) so the wrapper can build instance endpoints from the topology it reads out of the database, and the failover plugin works as it does on a cluster endpoint.

Make sure the pattern is an instance template and not a cluster one. Instance endpoints have no `cluster-` part, so `?.cluster-XYZ.us-east-1.rds.amazonaws.com` builds hostnames that never resolve. Use `?.XYZ.us-east-1.rds.amazonaws.com` when the instances are still reachable at their RDS endpoints, or `?.my-company.com` when the instances are behind your own DNS as well.

> [!NOTE]
> `clusterInstanceHostPattern` supplies the instance endpoint template only. No endpoint-recognition check consults it, so setting it does not make the ❌ cells above start working.

To have an out-of-date endpoint replaced behind a CNAME, enable `initialConnection` and set `verifyInitialConnectionType`. Stale DNS correction does not cover this case, so without those two the endpoint is not corrected and the first failure on that connection triggers normal failover instead.

> [!WARNING]
> Leaving `clusterInstanceHostPattern` unset on an IP address or custom domain degrades the instance template to a bare `?`, so the endpoints built from topology are single-label names such as `instance-1`, which do not resolve. `verifyInitialConnectionType` rejects that combination up front; failover and read/write splitting accept it and find no usable host.

[^1]: The custom endpoint plugin only applies to an Aurora cluster custom endpoint. On any other endpoint there is no custom-endpoint membership to track.
[^2]: On a cluster endpoint the authentication token must be generated for the instance the connection resolves to. Include the `initialConnection` plugin so the cluster endpoint is resolved to an instance before the token is minted.
[^3]: Limitless only operates on the Aurora PostgreSQL Limitless DB shard group endpoint. Set `limitlessUseShardGroupUrl=false` to skip that check and connect through another endpoint form.
[^4]: AWS does not support Blue/Green Deployments on Aurora Global Databases.
[^5]: These plugins do not inspect the host, so every endpoint type behaves the same.
[^6]: Set `verifyInitialConnectionType`. A custom endpoint name does not state a role, so the plugin performs no verification without it.
[^7]: Verification here always replaces the host with one selected from topology, which would route past the instance you named or bypass the proxy or shard group. The plugin takes no action on these endpoints and logs a warning if `verifyInitialConnectionType` is set.
[^8]: Stale DNS correction detects a writer cluster endpoint that resolved to a reader, so it only applies to a writer cluster endpoint.
[^9]: Host monitoring probes a specific database instance. An RDS Proxy endpoint hides the individual instances behind the proxy and a Limitless DB shard group endpoint exposes no per-instance topology, so the plugin monitors the endpoint in your DSN rather than an instance. It is not disabled, and the health it reports is the endpoint's, not a node's.
[^10]: Failover is disabled on an RDS Proxy endpoint; the proxy performs its own failover.
[^11]: Limitless shard groups manage their own routing and expose no per-instance topology for these plugins to work from. Use the `limitless` plugin instead. The wrapper does not block these plugins on a shard group endpoint, so the combination is unsupported rather than rejected.
[^12]: RDS Proxy re-routes each request to an instance it selects and hides the cluster topology, so topology-based read/write splitting cannot reliably route reads to a reader and writes to the writer. Connect through the proxy's own read/write endpoints instead.
[^13]: AWS Blue/Green Deployments are not available for RDS Multi-AZ DB clusters.
[^14]: Blue/Green is not supported on the Aurora Limitless DB shard group endpoint.
[^15]: Host monitoring falls back to the host in your DSN rather than the instance behind it, because it cannot resolve an unrecognized host to an instance. If DNS re-resolves, monitoring and the connection can end up pointed at different instances.
[^16]: Set [`clusterInstanceHostPattern`](./using-plugins/UsingTheFailoverPlugin.md#failover-parameters) so the wrapper can build instance endpoints from the topology. An IP address or custom domain cannot be pattern-matched to instance hostnames on its own.
[^17]: An IP address or custom domain does not encode the RDS hostname or region, so supply them explicitly with `iamHost` and `iamRegion`.
[^18]: Set both [`clusterInstanceHostPattern`](./using-plugins/UsingTheFailoverPlugin.md#failover-parameters) and `verifyInitialConnectionType`. The host states no role, so the parameter is the only way to declare one, and the pattern is what lets the plugin build the instance endpoint it substitutes. The connection fails immediately if the pattern is unset.
[^19]: The Blue/Green plugin resolves the deployment from a recognizable RDS endpoint, so it cannot follow a switchover behind an IP address or a custom domain name.
[^20]: The wrapper cannot know what role to expect from an unrecognized host, so there is nothing to detect. See [Connecting Through a Custom Domain](#connecting-through-a-custom-domain).
[^21]: The internal connection pool only accepts RDS and Aurora hosts.
[^22]: Connections are tracked, but host aliases are not resolved for a shard group endpoint, so a connection is matched by its endpoint rather than by every name the instance answers to.
