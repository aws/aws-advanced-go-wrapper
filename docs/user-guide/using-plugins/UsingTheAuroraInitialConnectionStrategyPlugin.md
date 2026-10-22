# Aurora Initial Connection Strategy Plugin
The Aurora Initial Connection Strategy Plugin allows users to configure their initial connection strategy, and helps obtain connections more reliably during DNS updates by replacing an out-of-date endpoint. When the Aurora Initial Connection Strategy Plugin attempts to make a connection, it may retry the connection attempt if there is a failure. Users can configure the retry frequency and maximum time allowed to obtain a connection using the connection parameters.

When this plugin is enabled and the initial connection is to a reader cluster endpoint, the connected reader host will be chosen based on the configured strategy. The [initial connection strategy](../ReaderSelectionStrategies.md) specifies how the driver determines which available reader to connect to.

This plugin also helps retrieve connections more reliably. When a user connects to a cluster endpoint, the actual instance for a new connection is resolved by DNS. During failover, the cluster elects another instance to be the writer. While DNS is updating, which can take up to 40-60 seconds, if a user tries to connect to the cluster endpoint, they may be connecting to an old node. This plugin helps by replacing the out-of-date endpoint if DNS is updating.

## Enabling the Aurora Initial Connection Strategy Plugin

To enable the Aurora Initial Connection Strategy Plugin, add `initialConnection` to the [`plugins`](../UsingTheGoWrapper.md#connection-plugin-manager-parameters) value.

## Aurora Initial Connection Strategy Connection Parameters

The following properties can be used to configure the Aurora Initial Connection Strategy Plugin.

| Parameter                                     |  Value  | Required | Description                                                                                                                                                                                                              | Example            | Default Value |
|-----------------------------------------------|:-------:|:--------:|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------|---------------|
| `readerInitialConnectionHostSelectorStrategy` | String  |    No    | The strategy that will be used to select a new reader host when opening a new connection. <br><br> For more information on the available reader selection strategies, see this [table](../ReaderSelectionStrategies.md). | `roundRobin`       | `random`      |
| `initialConnectionRetryTimeoutMs`                | Integer |    No    | The maximum allowed time for retries when opening a connection in milliseconds.                                                                                                                                          | `40000`            | `30000`       |
| `initialConnectionRetryIntervalMs`               | Integer |    No    | The time between retries when opening a connection in milliseconds.                                                                                                                                                      | `2000`             | `1000`        |
| `verifyInitialConnectionType`                 | String  | If connecting through a custom domain or IP address and you want the connection role verified: Yes<br><br>Otherwise: No | Requires an opened connection to have a given role, retrying until it does. Accepted values are `writer`, `reader` and `none`; any other value is rejected. `none` is equivalent to leaving it unset, except that it also switches verification **off** for endpoints that would otherwise be verified automatically. See [Endpoint types](#endpoint-types). | `reader`           | `` (no verification) |

## Endpoint types

What this plugin does depends on the endpoint in the connection string, since that is what states the role to expect.

| Endpoint | Behaviour |
|---|---|
| Aurora writer cluster endpoint, Aurora Global Database endpoint | Verifies the writer role. `verifyInitialConnectionType` is not needed. |
| Aurora reader cluster endpoint | Verifies the reader role and selects a reader using `readerInitialConnectionHostSelectorStrategy`. |
| Aurora cluster custom endpoint | No role is implied, so nothing is verified unless `verifyInitialConnectionType` is set. |
| Custom domain (CNAME alias) or IP address | No role is implied. Set `verifyInitialConnectionType`, and set [`clusterInstanceHostPattern`](./UsingTheFailoverPlugin.md#failover-parameters) so instance endpoints can be built from the topology. Without the host pattern the connection fails with an error naming it. |
| Instance endpoint, RDS Proxy endpoint, Aurora Limitless shard group endpoint | Connected to as given. An instance endpoint already names one host, and the other two are routing layers that substituting an instance would bypass. `verifyInitialConnectionType` is ignored and a warning is logged. |

If the requested role never appears within `initialConnectionRetryTimeoutMs`, the connection fails rather than returning a connection whose role was not confirmed.

See [Database URL type compatibility](../CompatibilityEndpoints.md) for the same information across every plugin.

> [!NOTE]\
> This plugin is **not** enabled by default. Add `initialConnection` to the [`plugins`](../UsingTheGoWrapper.md#connection-plugin-manager-parameters) parameter to use it.

### Connecting through a custom domain

A CNAME pointing at your writer cluster endpoint hides the endpoint type, so the wrapper cannot tell that a writer is expected. Set the role yourself to get stale DNS corrected after a failover:

```
host=db.mycompany.com user=user dbname=database password=password \
  plugins=initialConnection,failover \
  clusterInstanceHostPattern=?.abc123.us-east-1.rds.amazonaws.com \
  verifyInitialConnectionType=writer
```

`clusterInstanceHostPattern` describes your instance endpoints. A CNAME onto the cluster endpoint does not move the instances, so the real RDS instance pattern is usually the right value. Use a custom pattern only if instance DNS is also customized.
