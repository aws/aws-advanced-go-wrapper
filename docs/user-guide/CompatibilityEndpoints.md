# Database URL types compatibility

Not every plugin works with every kind of endpoint. This page lists the endpoint types you can put in a connection string and which plugins support each one.

Several plugins decide what to do from the shape of the hostname: a writer cluster endpoint implies a writer, a reader cluster endpoint implies a reader. A custom domain or an IP address implies nothing, so those plugins need the missing information supplied as parameters.

## Endpoint types

- [Aurora Global Database endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-global-database-connecting.html) - `<global-db-name>.global-<XYZ>.global.rds.amazonaws.com`
- [Aurora writer cluster endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Endpoints.Cluster.html) - `<cluster-name>.cluster-<XYZ>.<region>.rds.amazonaws.com`
- [Aurora reader cluster endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Endpoints.Reader.html) - `<cluster-name>.cluster-ro-<XYZ>.<region>.rds.amazonaws.com`
- [Aurora cluster custom endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Endpoints.Custom.html) - `<endpoint-name>.cluster-custom-<XYZ>.<region>.rds.amazonaws.com`
- [Instance endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Endpoints.Instance.html) - `<instance-name>.<XYZ>.<region>.rds.amazonaws.com`
- [RDS Multi-AZ DB cluster endpoints](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts-connection-management.html) - same shapes as the Aurora writer and reader cluster endpoints
- [RDS Proxy endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/rds-proxy-endpoints.html) - `<proxy-name>.proxy-<XYZ>.<region>.rds.amazonaws.com`
- [Aurora Limitless shard group endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/limitless-shard.html) - `<shard-group-name>.shardgrp-<XYZ>.<region>.rds.amazonaws.com`
- IP address - IPv4 or IPv6, for example `10.0.1.50`
- Custom domain (CNAME alias) - any other domain name, for example `db.mycompany.com`

> [!NOTE]\
> An Aurora cluster custom endpoint is a cluster endpoint with a user-defined member list, managed in RDS. A custom domain is a CNAME alias pointing at an RDS endpoint. Use the [Custom Endpoint Plugin](./using-plugins/UsingTheCustomEndpointPlugin.md) for the first and `clusterInstanceHostPattern` for the second.

## Aurora and RDS endpoints

| Plugin code | Aurora Global Database | Writer cluster | Reader cluster | Cluster custom | Instance |
|---|:---:|:---:|:---:|:---:|:---:|
| `failover` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `gdbFailover` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `efm` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `initialConnection` | ✅ | ✅ | ✅ | ✅ requires `verifyInitialConnectionType` | ❌ |
| `readWriteSplitting` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `gdbReadWriteSplitting` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `auroraConnectionTracker` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `customEndpoint` | ❌ | ❌ | ❌ | ✅ | ❌ |
| `limitless` | ❌ | ❌ | ❌ | ❌ | ❌ |
| `bg` | ❌ | ✅ | ✅ | ✅ | ✅ |
| `iam`, `federatedAuth`, `okta` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `awsSecretsManager` | ✅ | ✅ | ✅ | ✅ | ✅ |

Stale-DNS correction is part of `failover` rather than a separate plugin code. It applies to the writer cluster endpoint and the Aurora Global Database endpoint only, since those are the endpoints whose expected role the wrapper can determine from the hostname.

## RDS Proxy, Limitless, IP addresses and custom domains

| Plugin code | RDS Proxy | Limitless shard group | IP address | Custom domain (CNAME) |
|---|:---:|:---:|:---:|:---:|
| `failover` | ✅ | ❌ | ✅ requires `clusterInstanceHostPattern` and `clusterId` | ✅ requires `clusterInstanceHostPattern` and `clusterId` |
| `gdbFailover` | ✅ | ❌ | ✅ also requires `failoverHomeRegion` | ✅ also requires `failoverHomeRegion` |
| `efm` | ❌ | ❌ | ✅ | ✅ monitors the given host; see note below |
| `initialConnection` | ❌ | ❌ | ✅ requires `verifyInitialConnectionType` and `clusterInstanceHostPattern` | ✅ requires `verifyInitialConnectionType` and `clusterInstanceHostPattern` |
| `readWriteSplitting` | ❌ | ❌ | ✅ | ✅ |
| `gdbReadWriteSplitting` | ❌ | ❌ | ✅ also requires `gdbRwHomeRegion` | ✅ also requires `gdbRwHomeRegion` |
| `auroraConnectionTracker` | ✅ | ❌ | ✅ | ✅ |
| `customEndpoint` | ❌ | ❌ | ❌ | ❌ |
| `limitless` | ❌ | ✅ | ❌ | ❌ |
| `bg` | ✅ | ❌ | ✅ | ❌ |
| `iam`, `federatedAuth`, `okta` | ✅ | ✅ | ✅ requires `iamHost` and `iamRegion` | ✅ requires `iamHost` and `iamRegion` |
| `awsSecretsManager` | ✅ | ✅ | ✅ | ✅ |

Stale-DNS correction does not apply to any endpoint in this table, including a CNAME pointing at a writer cluster endpoint. Use `initialConnection` with `verifyInitialConnectionType=writer` instead: see [Connecting through a custom domain](./using-plugins/UsingTheAuroraInitialConnectionStrategyPlugin.md#connecting-through-a-custom-domain).

`efm` resolves the host it monitors to a specific instance only for cluster endpoints. On a custom domain it monitors the host as given, so if that name is a CNAME onto an Aurora cluster endpoint, monitoring follows DNS and may watch a different instance than the connection is using. Enable `initialConnection` with `verifyInitialConnectionType` to have the host replaced with an instance endpoint before monitoring starts. A custom domain naming a single database is monitored correctly as given.

`bg` identifies blue and green hosts by their endpoint names, so it does not support CNAME aliases. See [UsingTheBlueGreenPlugin.md](./using-plugins/UsingTheBlueGreenPlugin.md).

## Parameters referenced above

| Parameter | Why it is needed |
|---|---|
| [`clusterInstanceHostPattern`](./using-plugins/UsingTheFailoverPlugin.md#failover-parameters) | Builds instance endpoints from topology rows when the connection string is not a recognised RDS endpoint. Without it the wrapper produces unresolvable hostnames. |
| [`clusterId`](./ClusterId.md) | Keys the topology cache. Non-standard endpoints cannot be mapped to a cluster reliably, so set it explicitly. |
| [`verifyInitialConnectionType`](./using-plugins/UsingTheAuroraInitialConnectionStrategyPlugin.md#endpoint-types) | States the role you expect when the endpoint does not imply one. |
| [`iamHost`, `iamRegion`](./using-plugins/UsingTheIamAuthenticationPlugin.md) | IAM tokens must be signed with the real RDS hostname and region. |
| [`failoverHomeRegion`](./using-plugins/UsingTheGdbFailoverPlugin.md), [`gdbRwHomeRegion`](./using-plugins/UsingTheGdbReadWriteSplittingPlugin.md) | Needed when the region cannot be parsed from the endpoint. |
