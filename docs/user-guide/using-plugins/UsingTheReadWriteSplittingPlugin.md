## Read/Write Splitting Plugin

The Read/Write Splitting Plugin adds functionality to switch between writer and reader instances by setting context values in `QueryContext` and `ExecContext` calls. By setting the context value `awsctx.SetReadOnly` to `true`, the plugin will establish a connection to a reader instance and direct the query to this instance. The plugin will switch between writer and reader connections based on the context value.

### Loading the Read/Write Splitting Plugin

The Read/Write Splitting Plugin is not loaded by default. To load the plugin, include `readWriteSplitting` in the [`plugins`](../UsingTheGoWrapper.md#connection-plugin-manager-parameters) connection parameter:

### Supplying the connection string

When using the Read/Write Splitting Plugin against Aurora clusters, you do not have to supply multiple instance URLs in the connection string. Instead, supply only the URL for the initial connection. The Read/Write Splitting Plugin will automatically discover the URLs for the other instances in the cluster and will use this info to switch between the writer/reader when `awsctx.setReadOnly` is set.

> [!IMPORTANT]
> You must set the [`clusterInstanceHostPattern`](./UsingTheFailoverPlugin.md#failover-parameters) if you are connecting using an IP address or custom domain.

### Using the Read/Write Splitting Plugin

To use read-only queries with the plugin, set the context with key-value of `awsctx.SetReadOnly` to `true` when calling the `QueryContext`, or `ExecContext`. This context value must be supplied for every call in QueryContext or ExecContext. If no context is supplied, then the driver will treat SetReadOnly to `false`. 

```go
ctx := context.WithValue(context.Background(), awsctx.SetReadOnly, true)
// Both calls will be directed to a reader instance.
rows, queryErr := conn.QueryContext(ctx, "SELECT 1") 
result, execErr := ExecContext(ctx, "SELECT 1")
```


### Using the Read/Write Splitting Plugin against non-Aurora clusters

The Read/Write Splitting Plugin is not currently supported for non-Aurora clusters.

## Internal Connection Pooling

> [!WARNING]
> If internal connection pools are enabled, database passwords may not be verified with every connection request. The initial connection request for each database instance in the cluster will verify the password, but subsequent requests may return a cached pool connection without re-verifying the password. This behavior is inherent to the nature of connection pools in general and not a bug with the wrapper. `<name-of-ConnectionProvider>.ReleaseResources()` can be called to close all pools and remove all cached pool connections. See [Internal Connection Pool Password Warning Example for Postgres](../../../examples/aws_internal_connection_pool_password_warning_postgresql_example.go) and [Internal Connection Pool Password Warning Example for MySQL](../../../examples/aws_internal_connection_pool_password_warning_mysql_example.go)

When `awsctx.setReadOnly=true` is first passed in on a `sql.Conn` object the read/write plugin will internally open a new physical connection to a reader. After this first call, the physical reader connection will be cached. Future calls passing `awsctx.setReadOnly=true` on the same `sql.Conn` object will not require opening a new physical connection. However, calling `awsctx.setReadOnly=true` for the first time on a new `sql.Conn` object, even one from the same `sql.DB` object, will require the plugin to establish another new physical connection to a reader.

Due to the abstraction of go's `database/sql` library, many users may often find themselves executing queries with the `sql.DB` object. `sql.DB` is a high-level connection pool, users do not have control of which `sql.Conn` is being used when running queries using methods on this object (such as`db.ExecContext`). That is, if a user were to run `db.ExecContext` multiple times, each of those times could result in creating a new connection object and making a new physical connection to the reader.

If your application uses `sql.DB`, then you can enable internal connection pooling to improve performance. When enabled, the wrapper driver will maintain an internal connection pool for each instance in the cluster. This allows the read/write splitting plugin to reuse connections that were established by queries using `awsctx.setReadOnly=true`.

> [!NOTE]
> Initial connections to a cluster URL will not be pooled. The driver does not pool cluster URLs because it can be problematic to pool a URL that resolves to different instances over time. The main benefit of internal connection pools is when setReadOnly is called. When setReadOnly is called (regardless of the initial connection URL), an internal pool will be created for the writer/reader that the plugin switches to and connections for that instance can be reused in the future.

The wrapper driver creates and maintain its internal connection pools using. The steps to set this up are as follows:

1.  Create an instance of `InternalPooledConnectionProvider`.

```go
import "github.com/aws/aws-advanced-go-wrapper/awssql/internal_pool"

poolOptions := internal_pool.NewInternalPoolOptions(
		internal_pool.WithMaxIdleConns(2),
		internal_pool.WithMaxConnLifetime(time.Duration(10)*time.Minute),
		internal_pool.WithMaxConnIdleTime(time.Duration(10)*time.Minute),
)

provider := internal_pool.NewInternalPooledConnectionProvider(
  poolOptions,
  time.Duration(30) * time.Minute, 
)
```


The following table outlines the options for `InternalPoolConfig`

| InternalPoolConfig Parameter      |  Value  | Required | Description                                                                                                                                                                                                                                                            | Default Value            |
| -------------------- | :-----: | :------: | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------ |
| `maxIdleConns`     | int  |    No    | The maximum number of connections to create.                                                                                                                                                                                                                           | `2`                     |
| `maxConnLifetime`  | int  |    No    | The maximum life a connection object can exist                                                                                                                                                                                                         | `0` (no time limit)                  |
| `maxConnIdleTime` | int  |    No    | The maximum duration that a connection object can be idle in the pool                                                                                     | `0` (no time limit)|


To avoid collisions, the `InternalPooledConnectionProvider` uses a function called poolKeyFunc to retrieve a value from properties map or host info. The value for this gets combined with the host url, and the underlying database driver that's being used. By default, the driver uses the values from `user`, or `dbUser` if `user` does not exist, from the connection string. If needed, users can override this to get a different property by using the `NewInternalPooledConnectionProviderWithPoolKeyFunc()` constructor like below:

```go
// keyFunc will use the value for plugins in place of the user value in the pool key
// Note: Signature of keyFunc must match type of internal_pool.internalPoolKeyFunc
keyFunc := func(host *host_info_util.HostInfo, props map[string]string) string {
    return props["plugins"]
}

provider := internal_pool.NewInternalPooledConnectionProviderWithPoolKeyFunc(
  poolOptions,
  0,
  keyFunc,
)
```

Please see [Internal Connection Pooling Postgres Example](../../../examples/read_write_splitting_postgresql_example.go) and [Internal Connection Pooling MySQL Example](../../../examples/read_write_splitting_mysql_example.go) for the full examples.



> [!WARNING]
> If you do not include the username in your InternalPoolMapping function, connection pools may be shared between different users. As a result, an initial connection established with a privileged user may be returned to a connection request with a lower-privilege user without re-verifying credentials. This behavior is inherent to the nature of connection pools in general and not a bug with the driver. `provider.ReleaseResources()` can be called to close all pools and remove all cached pool connections.

2. Set the connection provider by running `driver_infrastructure.SetCustomConnectionProvider(provider)`.

3. By default, the read/write plugin randomly selects a reader instance the first time a query runs with a context containg a key-value pair of `awsctx.setReadOnly` to true. If you would like the plugin to select a reader based on a different selection strategy, please see the [Reader Selection](#reader-selection) section for more information.

4. Continue as normal: create connections and use them as needed. Signal using `awsctx.setReadOnly` whether you want each operation to be on a reader or writer instance. Remember that not providing this value is equivalent to setting `awsctx.setReadOnly` to false.

5. When you are finished using all connections, call `driver.ReleaseResources()`, and then `provider.ReleaseResources()`.

> [!IMPORTANT]
> To ensure the provider can close the pools, call `driver.ClearCaches()` first.
>
> You must call `provider.ReleaseResources()` to close the internal connection pools when you are finished using all connections. Unless `provider.ReleaseResources()` is called, the wrapper driver will keep the pools open so that they can be shared between connections.

### Reader Selection

By default, the Read/Write Splitting Plugin randomly selects a reader instance the first time `awsctx.SetReadOnly` is set to true. To balance connections to reader instances more evenly, different connection strategies can be used. The following table describes the currently available connection strategies and any relevant configuration parameters for each strategy.

To indicate which connection strategy to use, the `readerHostSelectorStrategy` parameter can be set to one of the [reader host selection strategies](../ReaderSelectionStrategies.md). The following is an example of enabling the `random` strategy:

```go
host := "endpoint"
port := "3306"
user := "user"
password := "password"
dbName := "db"
readerHostStrategy := "random"

// If using MySQL:
connStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s&readerHostSelectorStrategy=%s",
  user, password, host, port, dbName, readerHostStrategy,
)

// If using Postgres:
connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s readerHostSelectorStrategy=%s",
  host, port, user, password, dbName, readerHostStrategy,
)
```

> [!WARNING]
> Connections with the Read/Write Splitting Plugin may have cached resources used throughout multiple connections. To clean up any resources used by the plugins at the end of the application call `driver.ClearCaches()`.