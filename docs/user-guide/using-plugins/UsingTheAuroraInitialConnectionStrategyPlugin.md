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
| `verifyInitialConnectionType`                 | String  |    No    | Verify that an opened connection is to a `writer` or to a `reader`, and replace it with one of that role if it is not. Set it to `none`, or leave it unset, to perform no role verification. | `reader`           | ``            |

## Supported Endpoints

The plugin infers the role it should verify from the initial connection host. A cluster endpoint and a
read-only cluster endpoint name their role, so the plugin verifies them without any extra
configuration. An Aurora cluster custom endpoint, an IP address and a custom domain do not provide any information about the connection role, so they are verified only when you set `verifyInitialConnectionType`.

To connect through a CNAME in front of a cluster endpoint and still have the out-of-date endpoint
replaced, set both parameters:

```
host=db.example.com clusterInstanceHostPattern=?.example.com verifyInitialConnectionType=writer
```

Any value other than `writer`, `reader` and `none` is treated as unset, so no verification is
performed.

> [!WARNING]
> If you set `verifyInitialConnectionType` and verification does not succeed within
> `initialConnectionRetryTimeoutMs`, the connection fails with an error rather than returning a
> connection of the role you excluded. Where the role comes from the endpoint name instead of this
> parameter, an exhausted timeout still returns the connection opened through the original endpoint.
