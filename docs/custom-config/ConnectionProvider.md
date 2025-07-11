# Connection Provider

### Applicable plugins: DefaultPlugin

A connection provider helps the wrapper connect to the database and helps select which host to connect to. If you would like to use your own connection provider you can do so by first creating a struct that implements the `ConnectionProvider` interface. Then, call the `driver_infrastructure.SetCustomConnectionProvider` method and pass in the struct that implements [`ConnectionProvider`](../../awssql/driver_infrastructure/connection_provider.go). For more details on wrapper behaviour and the impacted pipelines see: [Pipelines](../contributor-guide/Pipelines.md).

## Connect Pipeline
By default, the connect pipeline will establish connections using the `DriverConnectionProvider` class. Once a custom connection provider is set, connection requests will attempt to connect through the custom provider. The `AcceptsUrl` method of `ConnectionProvider` filters which connections are supported by the provider, and the `Connect` method makes the connection to the database. `Connect` will only be called for a set of parameters if `AcceptsUrl` returns `true`. If you do not want to use your custom connection provider to connect, return `false` in `AcceptsUrl`. 

> [!NOTE]
> The force connect pipeline will not be impacted. 

## AcceptsStrategy and GetHostInfoByStrategy Pipelines
By default, host selection will establish connections using the `DriverConnectionProvider` class and the [random](../user-guide/ReaderSelectionStrategies.md#reader-selection-strategies) selection strategy. 

> [!NOTE]
> Host selection will first pass through other plugins that implement the `AcceptsStrategy` and `GetHostInfoByStrategy` pipelines. For example, the `limitless` plugin uses its own host selection strategies. For more information see the [Limitless](../user-guide/using-plugins/UsingTheLimitlessConnectionPlugin.md) and [Pipelines](../contributor-guide/Pipelines.md) documentation. 

Once a custom connection provider is set, connection requests will attempt to select a host through the custom provider. The `AcceptsStrategy` method of `ConnectionProvider` filters which strategies are supported by the provider, the `GetHostSelectorStrategy` method returns a `HostSelector` that implements a strategy, and the `GetHostInfoByStrategy` method selects a host from a given list of available hosts. For any given strategy, `GetHostInfoByStrategy` and `GetHostSelectorStrategy` will only be called if `AcceptsStrategy` returns true. To create a new host selection strategy implement the [`HostSelector`](../../awssql/driver_infrastructure/host_selector.go) interface. If you do not want to use your custom connection provider to select hosts, return `false` in `AcceptsStrategy`. 

### Use Case: Host Selection with the Failover Plugin
During failover, connections to the database change. Under the `failoverMode` of `strict-reader` or `reader-or-writer`, a reader host is selected using the strategy set by the property `failoverReaderHostSelectorStrategy`. To use a custom host selection strategy in the case of a failover event: 
1. Create a custom `HostSelector` that implements a strategy `x`
2. Create a custom `ConnectionProvider` that supports strategy `x` and uses the custom host selector
3. Call `driver_infrastructure.SetCustomConnectionProvider` and pass in the custom connection provider
4. Set the `failoverMode` property to either `strict-reader` or `reader-or-writer`
5. Set the`failoverReaderHostSelectorStrategy` property to `x`
6. Connect using a valid DSN with the properties set in step 4 and 5 and the `failover` plugin enabled

When failover occurs, the wrapper will attempt to connect to reader hosts in the order determined by `x`. See the [Failover documentation](../user-guide/using-plugins/UsingTheFailoverPlugin.md) for more details on failover related properties and behaviour.

> [!NOTE]
> Failover under the mode `strict-writer` will not be impacted.

## Sample code

[Custom implementation of ConnectionProvider](./../../examples/custom_connection_provider_example.go)