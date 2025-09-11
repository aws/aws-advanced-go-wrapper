# Blue/Green Deployment Plugin

## What is Blue/Green Deployment?

The [Blue/Green Deployment](https://docs.aws.amazon.com/whitepapers/latest/blue-green-deployments/introduction.html) technique enables organizations to release applications by seamlessly shifting traffic between two identical environments running different versions of the application. This strategy effectively mitigates common risks associated with software deployment, such as downtime and limited rollback capability.

The AWS Advanced Go Wrapper leverages the Blue/Green Deployment approach by intelligently managing traffic distribution between blue and green hosts, minimizing the impact of stale DNS data and connectivity disruptions on user applications.

**Important: Service Dependency**

Support for Blue/Green deployments using the AWS Advanced Go Wrapper requires specific metadata tables that are **not available in the current RDS and Aurora service**. Please contact your AWS account team for metadata release timelines.

## Prerequisites
> [!WARNING]
> Currently Supported Database Deployments:
> - Aurora MySQL and PostgreSQL clusters
> - RDS MySQL and PostgreSQL instances
>
> Unsupported Database Deployments and Configurations:
> - RDS MySQL and PostgreSQL Multi-AZ clusters
> - Aurora Global Database for MySQL and PostgreSQL
>
> Additional Requirements:
> - AWS cluster and instance endpoints must be directly accessible from the client side
> - Connecting to database hosts using CNAME aliases is not supported
>
> **Blue/Green Support Behaviour and Version Compatibility:**
>
> The AWS Advanced Go Wrapper now includes enhanced full support for Blue/Green Deployments. This support requires a minimum database version that includes a specific metadata table. This constraint **does not** apply to RDS MySQL.
>
> If your database version does **not** support this table, the driver will automatically detect its absence and fallback to its previous behaviour. In this fallback mode, Blue/Green handling is subject to the same limitations listed above.
>
> **No action is required** if your database does not include the new metadata table -- the driver will continue to operate as before. If you have questions or encounter issues, please open an issue in this repository.
>
> Supported RDS PostgreSQL Versions: `rds_tools v1.7 (17.1, 16.5, 15.9, 14.14, 13.17, 12.21)` and above.<br>
> Supported Aurora PostgreSQL Versions: Engine Release `17.5, 16.9, 15.13, 14.18, 13.21` and above.<br>
> Supported Aurora MySQL Versions: Engine Release `3.07` and above.

## What is the Blue/Green Deployment Plugin?

During a [Blue/Green switchover](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/blue-green-deployments-switching.html), several significant changes occur to your database configuration:
- Connections to blue hosts terminate during the transition
- Host connectivity may be temporarily impacted due to reconfigurations and potential host restarts
- Cluster and instance endpoints are redirected to different database hosts
- Internal database host names change
- Internal security certificates are regenerated to accommodate the new host names

All factors mentioned above may cause application disruption. The AWS Advanced Go Wrapper aims to minimize the application disruption during Blue/Green switchover by performing the following actions:
- Actively monitors Blue/Green switchover status and implements appropriate measures to suspend, pass-through, or re-route database traffic
- Prior to Blue/Green switchover initiation, compiles a comprehensive inventory of cluster and instance endpoints for both blue and green hosts along with their corresponding IP addresses
- During the active switchover phase, temporarily suspends execution of method calls to blue hosts, which helps unload database hosts and reduces transaction lag for green hosts, thereby enhancing overall switchover performance
- Substitutes provided hostnames with corresponding IP addresses when establishing new blue connections, effectively eliminating stale DNS data and ensuring connections to current blue hosts
- During the brief post-switchover period, continuously monitors DNS entries, confirms that blue endpoints have been reconfigured, and discontinues hostname-to-IP address substitution as it becomes unnecessary
- Automatically rejects new connection requests to green hosts when the switchover is completed but DNS entries for green hosts remain temporarily available
- Intelligently detects switchover failures and rollbacks to the original state, implementing appropriate connection handling measures to maintain application stability


## How do I use the Blue/Green Deployment Plugin with the AWS Advanced Go Wrapper?

To enable the Blue/Green Deployment functionality, add the plugin code `bg` to the [`plugins`](../UsingTheGoWrapper.md#connection-plugin-manager-parameters) parameter value.
The Blue/Green Deployment Plugin supports the following configuration parameters:

| Parameter                     |  Value  |                           Required                           | Description                                                                                                                                                                                                                                                                                                                                                                                                                                             | Example Value            | Default Value |
|-------------------------------|:-------:|:------------------------------------------------------------:|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------|---------------|
| `bgdId`                       | String  | If using multiple Blue/Green Deployments, yes; otherwise, no | This parameter is optional and defaults to `1`. When supporting multiple Blue/Green Deployments (BGDs), this parameter becomes mandatory. Each connection string must include the `bgdId` parameter with a value that can be any number or string. However, all connection strings associated with the same Blue/Green Deployment must use identical `bgdId` values, while connection strings belonging to different BGDs must specify distinct values. | `1234`, `abc-1`, `abc-2` | `1`           |
| `bgConnectTimeoutMs`          | Integer |                              No                              | Maximum waiting time (ms) for establishing new connections during a Blue/Green switchover when blue and green traffic is temporarily suspended.                                                                                                                                                                                                                                                                                                         | `30000`                  | `30000`       |
| `bgBaselineMs`                | Integer |                              No                              | The baseline interval (ms) for checking Blue/Green Deployment status. It's highly recommended to keep this parameter below 900000ms (15 minutes).                                                                                                                                                                                                                                                                                                       | `60000`                  | `60000`       |
| `bgIncreasedMs`               | Integer |                              No                              | The increased interval (ms) for checking Blue/Green Deployment status. Configure this parameter within the range of 500-2000 milliseconds.                                                                                                                                                                                                                                                                                                              | `1000`                   | `1000`        |
| `bgHighMs`                    | Integer |                              No                              | The high-frequency interval (ms) for checking Blue/Green Deployment status. Configure this parameter within the range of 50-500 milliseconds.                                                                                                                                                                                                                                                                                                           | `100`                    | `100`         |
| `bgSwitchoverTimeoutMs`       | Integer |                              No                              | Maximum duration (ms) allowed for switchover completion. If the switchover process stalls or exceeds this timeframe, the driver will automatically assume completion and resume normal operations.                                                                                                                                                                                                                                                      | `180000`                 | `180000`      |
| `bgSuspendNewBlueConnections` | Boolean |                              No                              | Enables Blue/Green Deployment switchover to suspend new blue connection requests while the switchover process is in progress.                                                                                                                                                                                                                                                                                                                           | `false`                  | `false`       |

The plugin establishes dedicated monitoring connections to track Blue/Green Deployment status. To apply specific configurations to these monitoring connections, add the `blue-green-monitoring-` prefix to any configuration parameter, as shown in the following example:

```go
// Configure different timeout values for the non-monitoring and monitoring connections.
pgxDsn := "... connect_timeout=30 blue-green-monitoring-connect_timeout=10"
```

> [!WARNING]
> **Always ensure you provide a non-zero connect timeout value to the Blue/Green Deployment Plugin**
>

### Connections with the IAM Authentication Plugin

When connecting with the IAM Authentication Plugin, additional permissions for the IAM user may be required. See [Connecting with Multi-AZ or Blue/Green Deployments](UsingTheIamAuthenticationPlugin.md#connecting-with-multi-az-or-bluegreen-deployments) for specifics.

## Plan your Blue/Green switchover in advance

To optimize Blue/Green switchover support with the AWS Advanced Go Wrapper, advance planning is essential. Please follow these recommended steps:

1. Create a Blue/Green Deployment for your database.
2. Connect with the `bg` plugin along with any additional parameters of your choice set in your DSN.
3. The order of steps 1 and 2 is flexible and can be performed in either sequence.
4. Allow sufficient time for the deployed application with the active Blue/Green plugin to collect deployment status information. This process typically requires several minutes.
5. Initiate the Blue/Green Deployment switchover through the AWS Console, CLI, or RDS API.
6. Monitor the process until the switchover completes successfully or rolls back. This may take several minutes.
7. Review the switchover summary in the application logs. This requires setting the log level to `Debug`. Alternatively, a simple summary will be logged under the level `Info`. See [Logging](./../UsingTheGoWrapper.md#logging) for how to set the log level.
8. Update your application by removing the `bg` plugin from your DSN. Redeploy your application afterward. Note that an active Blue/Green plugin produces no adverse effects once the switchover has been completed.
9. Delete the Blue/Green Deployment through the appropriate AWS interface.
10. The sequence of steps 8 and 9 is flexible and can be executed in either order based on your preference.

Here's an example of a switchover summary. Time zero corresponds to the beginning of the active switchover phase. Time offsets indicate the start time of each specific switchover phase.
```
2025/09/05 17:56:30 INFO [bgdId: '1']
----------------------------------------------------------------------------------
timestamp                         time offset (ms)                           event
----------------------------------------------------------------------------------
  2025-09-05T17:52:07.953Z         -223508 ms                         CREATED
  2025-09-05T17:55:47.631Z           -3831 ms                     PREPARATION
  2025-09-05T17:55:51.463Z               0 ms                     IN_PROGRESS
  2025-09-05T17:55:54.011Z            2548 ms                            POST
  2025-09-05T17:56:05.660Z           14197 ms          Green topology changed
  2025-09-05T17:56:28.907Z           37444 ms                Blue DNS updated
  2025-09-05T17:56:29.480Z           38017 ms               Green DNS removed
  2025-09-05T17:56:30.626Z           39163 ms                       COMPLETED
----------------------------------------------------------------------------------
```

