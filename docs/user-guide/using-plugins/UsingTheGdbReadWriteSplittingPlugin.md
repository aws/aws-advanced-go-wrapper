# Global Database (GDB) Read/Write Splitting Plugin

The GDB Read/Write Splitting Plugin extends the functionality of the [Read/Write Splitting Plugin](./UsingTheReadWriteSplittingPlugin.md) and adopts some additional settings to improve support for Global Databases.

The GDB Read/Write Splitting Plugin adds the notion of a home region and allows users to constrain new connections to this region. Such restrictions may be helpful to prevent opening new connections in environments where remote AWS regions add substantial latency that cannot be tolerated.

Unless otherwise stated, all recommendations, configurations and code examples made for the [Read/Write Splitting Plugin](./UsingTheReadWriteSplittingPlugin.md) are applicable to the current GDB Read/Write Splitting Plugin.

## Loading the Global Database Read/Write Splitting Plugin

The GDB Read/Write Splitting Plugin is not loaded by default. To load the plugin, include it in the `plugins` connection parameter. If you would like to load the GDB Read/Write Splitting Plugin alongside the failover and host monitoring plugins, the GDB Read/Write Splitting Plugin **must be listed before** these plugins in the plugin chain. If it is not, failover exceptions will not be properly processed by the plugin. See the example below to properly load the GDB Read/Write Splitting Plugin with these plugins. Although the driver performs proper plugin sorting by default (see [`autoSortWrapperPluginOrder` configuration parameter](../UsingTheGoWrapper.md#connection-plugin-manager-parameters)), this note remains important.

```go
// MySQL
connStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?plugins=gdbReadWriteSplitting,efm,gdbFailover",
  user, password, host, port, dbName,
)
db, _ := sql.Open("awssql-mysql", connStr)

// PostgreSQL
connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s plugins=gdbReadWriteSplitting,efm,gdbFailover",
  host, port, user, password, dbName,
)
db, _ := sql.Open("awssql-pgx", connStr)
```

If you would like to use the GDB Read/Write Splitting Plugin without the failover plugin, make sure you have the `gdbReadWriteSplitting` plugin in the `plugins` parameter, and that the failover plugin is not part of it.

```go
connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s plugins=gdbReadWriteSplitting",
  host, port, user, password, dbName,
)
```

> [!WARNING]
> Do not use the `readWriteSplitting` and `gdbReadWriteSplitting` plugins at the same time for the same connection.

## Using the GDB Read/Write Splitting Plugin against non-GDB clusters

The GDB Read/Write Splitting Plugin can be used against Aurora clusters and RDS clusters. However, since these cluster types are single-region clusters, the notion of a home region is not meaningful. Using the Read/Write Splitting Plugin is recommended instead.

## Configuration Parameters

| Parameter                         |  Value  |                                                                  Required                                                                   | Description                                                                                                                                                                                                                                                                                                                                                                                            | Default Value                                                                                                                    |
|-----------------------------------|:-------:|:-------------------------------------------------------------------------------------------------------------------------------------------:|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------|
| `readerHostSelectorStrategy`      | String  |                                                                     No                                                                      | The name of the strategy that should be used to select a new reader host. For more information on the available reader selection strategies, see this [table](../ReaderSelectionStrategies.md).                                                                                                                                                                                                          | `random`                                                                                                                         |
| `gdbRwHomeRegion`                 | String  | If connecting using an IP address, a custom domain URL, Global Database endpoint or other endpoint with no region: Yes<br><br>Otherwise: No | Defines a home region.<br><br>Examples: `us-west-2`, `us-east-1`. <br><br>If this parameter is omitted, the value is parsed from the connection URL. If the provided endpoint has no region (for example, a Global Database endpoint or IP address), the configuration parameter is mandatory. | For regional cluster endpoints and instance endpoints, it's set to the region of the provided endpoint.<br><br>Otherwise: `null` |
| `gdbRwRestrictWriterToHomeRegion` | Boolean |                                                                     No                                                                      | If set to `true`, prevents following and connecting to a writer node outside the defined home region. An error will be returned when such a connection to a writer outside the home region is requested.                                                                                                                                                                                             | `true`                                                                                                                           |
| `gdbRwRestrictReaderToHomeRegion` | Boolean |                                                                     No                                                                      | If set to `true`, prevents connecting to a reader node outside the defined home region. If no reader nodes in the home region are available, an error will be returned.                                                                                                                                                                                                                              | `true`                                                                                                                           |

Please refer to the [Read/Write Splitting Plugin](./UsingTheReadWriteSplittingPlugin.md) for more details about error codes, configurations, connection pooling and sample code.
