# Aurora Global Databases

The AWS Advanced Go Wrapper provides comprehensive support for [Amazon Aurora Global Databases](https://aws.amazon.com/rds/aurora/global-database/), including both in-region and cross-region failover capabilities.

## Overview

Aurora Global Database is a feature that allows a single Aurora database to span multiple AWS regions. It provides fast replication across regions with minimal impact on database performance, enabling disaster recovery and serving read traffic from multiple regions.

The AWS Advanced Go Wrapper supports:
- In-region failover
- Cross-region planned failover and switchover
- Global Writer Endpoint recognition
- Stale DNS handling

## Configuration

The following instructions are recommended by AWS Service Teams for Aurora Global Database connections. This configuration provides writer connections with support for both in-region and cross-region failover.

### Writer Connections

**Connection String:**
Use the global cluster endpoint:
```
<global-db-name>.global-<XYZ>.global.rds.amazonaws.com
```

**Configuration Parameters:**

| Parameter                           | Value                                                                                                   | Notes                                                                                          |
|-------------------------------------|---------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------|
| `clusterId`                         | `1`                                                                                                     | See [clusterId parameter documentation](./ClusterId.md)                                        |
| `databaseDialect`                   | `global-aurora-mysql` or `global-aurora-pg`                                                             |                                                                                                |
| `plugins`                           | `failover,efm` or<br/>`gdbFailover,efm`                                                                | Without connection pooling                                                                     |
|                                     | `auroraConnectionTracker,failover,efm` or<br/>`auroraConnectionTracker,gdbFailover,efm`                | With connection pooling                                                                        |
| `globalClusterInstanceHostPatterns` | `?.XYZ1.us-east-2.rds.amazonaws.com,?.XYZ2.us-west-2.rds.amazonaws.com`                                | See [documentation](./using-plugins/UsingTheFailoverPlugin.md)                                 |

> **Note:** Add additional plugins according to the plugin compatibility requirements of your application.

### Reader Connections

**Connection String:**
Use the cluster reader endpoint:
```
<cluster-name>.cluster-ro-<XYZ>.<region>.rds.amazonaws.com
```

**Configuration Parameters:**

| Parameter                           | Value                                                                                                   | Notes                                    |
|-------------------------------------|---------------------------------------------------------------------------------------------------------|------------------------------------------|
| `clusterId`                         | `1`                                                                                                     | Use the same value as writer connections |
| `databaseDialect`                   | `global-aurora-mysql` or `global-aurora-pg`                                                             |                                          |
| `plugins`                           | `failover,efm` or<br/>`gdbFailover,efm`                                                                | Without connection pooling               |
|                                     | `auroraConnectionTracker,failover,efm` or<br/>`auroraConnectionTracker,gdbFailover,efm`                | With connection pooling                  |
| `globalClusterInstanceHostPatterns` | Same as writer configuration                                                                            |                                          |
| `failoverMode`                      | `strict-reader` or `reader-or-writer`                                                                   | Depending on system requirements         |

> **Note:** Add additional plugins according to the plugin compatibility requirements of your application.

## Example Configuration

- [MySQL Example](../../examples/global_databases_mysql_example.go)
- [PostgreSQL Example](../../examples/global_databases_postgresql_example.go)

## Important Considerations

### Plugin Selection
- **Connection Pooling**: Include `auroraConnectionTracker` plugin when using connection pooling
- `gdbFailover` plugin has extended failover functionality and supports application home region

### Global Cluster Instance Host Patterns
The `globalClusterInstanceHostPatterns` parameter is **required** for Aurora Global Databases. The patterns are based on instance endpoints. It should contain:
- Comma-separated list of host patterns for each region
- Different cluster identifiers for each region (e.g., `XYZ1`, `XYZ2`)
- Proper region specification for custom domains: `[us-east-1]?.custom.com`

### Failover Behavior
- **In-region failover**: Automatic failover within the same region
- **Cross-region failover**: Planned failover to a different region

### Authentication Plugins Compatible with GDB
- [IAM Authentication Plugin](./using-plugins/UsingTheIamAuthenticationPlugin.md)
- [Federated Authentication Plugin](./using-plugins/UsingTheFederatedAuthPlugin.md)
- [Okta Authentication Plugin](./using-plugins/UsingTheOktaAuthPlugin.md)
