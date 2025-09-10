## Read/Write Splitting Plugin

The Read/Write Splitting Plugin adds functionality to switch between writer and reader instances by setting context values in `QueryContext`, `ExecContext`, or `QueryRowContext` calls. By setting the context value `awsctx.SetReadOnly` to `true`, the plugin will establish a connection to a reader instance and direct the query to this instance. The plugin will switch between writer and reader connections based on the context value.

### Loading the Read/Write Splitting Plugin

The Read/Write Splitting Plugin is not loaded by default. To load the plugin, include `readWriteSplitting` in the [`plugins`](../UsingTheGoWrapper.md#connection-plugin-manager-parameters) connection parameter:

### Supplying the connection string

When using the Read/Write Splitting Plugin against Aurora clusters, you do not have to supply multiple instance URLs in the connection string. Instead, supply only the URL for the initial connection. The Read/Write Splitting Plugin will automatically discover the URLs for the other instances in the cluster and will use this info to switch between the writer/reader when `awsctx.setReadOnly` is set.

> [!IMPORTANT]
> You must set the [`clusterInstanceHostPattern`](./UsingTheFailoverPlugin.md#failover-parameters) if you are connecting using an IP address or custom domain.

### Using the Read/Write Splitting Plugin

To switch between a writer and a reader connection, a context key of `awsctx.SetReadOnly` is provided. This can be set to `true` to switch to a read-only connection, and `false` to switch back to the writer instance. These can be set using `QueryContext`, `ExecContext`, or `QueryRowContext`.  

In the `database/sql` library, there are 4 main types that are used to run a query: `sql.Conn`, `sql.Tx`, `sql.Stmt`, and `sql.DB`. Only the first 3 are recommended to be used with the Read/Write Splitting Plugin. Please see the following sections on how to use the plugin for each of these types.

#### sql.Conn

To switch between readers and writer, pass in `awsctx.SetReadOnly` to the first query of your batch, and any subsequent queries will run on that desired instance. 

**Always pass in `awsctx.SetReadOnly` in your first query, and reset session settings before closing. Failing to do so may result in running a query in the wrong mode. While the default behavior of a newly-opened connection is to connect to the writer, if a recycled connection is returned by `sql.DB`, the `sql.Conn` object might be in ReadOnly mode.**

**Furthermore, it is recommended to only pass in the `awsctx.SetReadOnly` setting when switching between reader and writer or vice versa. Passing in `awsctx.SetReadOnly` in every query when you are not switching does not change any results and there is some overhead when this value is passed in.**

The following is an example on how to use the plugin with `sql.Conn`.
```go
readOnlyCtx := context.WithValue(context.Background(), awsctx.SetReadOnly, true)
writeCtx := context.WithValue(context.Background(), awsctx.SetReadOnly, false)

db, _ := sql.Open("awssql-mysql", "dsn")
conn, _ := db.Conn(ctx)

result, err := conn.ExecContext(readOnlyCtx, "SELECT 1") // Will switch to reader
result, err = conn.QueryContext(context.TODO(), "SELECT 1") // Will query against reader instance
result, err = conn.Query("SELECT 1") // Will query against reader instance

result, err = conn.QueryContext(writeCtx, "INSERT INTO...") // Will switch to writer instance
result, err = conn.Exec("...") // Will execute against writer instance

// Reset session
conn.ResetSession()
conn.Close()
```


#### sql.Tx
`sql.Tx` objects should be created through the `BeginTx()` method. the plugin does not support read/write splitting for objects created with  `Begin()` because it is deprecated and does not allow users to pass in contexts, or options. The plugin provides 2 options to switch between the reader and writer instances:

1. Set the mode through `awsctx.SetReadOnly`
```go
readOnlyCtx := context.WithValue(context.Background(), awsctx.SetReadOnly, true)

db, _ := sql.Open("awssql-mysql", "dsn")
tx, _ := db.BeginTx(readOnlyCtx, nil) // Will connect to reader instance
result, err := conn.ExecContext(readOnlyCtx, "SELECT 1")
tx.Commit()
```

2. Set the mode using the `sql.TxOptions` parameter
```go
db, _ := sql.Open("awssql-mysql", "dsn")
tx, _ := db.BeginTx(context.TODO(), &sql.TxOptions{ReadOnly: true}) // will connect to reader instance
result, err := conn.ExecContext(readOnlyCtx, "SELECT 1")
tx.Commit()
```

If both are supplied, priority is given to the `awsctx.SetReadOnly` value. If none is given, it will use the writer connection.

After the `sql.Tx` is created, the transaction will run based on the mode specified in `BeginTx()`

> [!WARNING]
> **Do not  pass in `awsctx.SetReadOnly` when running queries through the tx object. This will result in an error as switching connections during a transaction is not allowed.**.

#### sql.Stmt

`sql.Stmt` objects are created through `PrepareContext()`. Similar to `sql.Tx`, the plugin does not support read/write splitting for objects created with  `Prepare()` as it is deprecated and does not allow users to pass in contexts. The plugin does not support switching after creating the `sql.Stmt` object as session states alterations are not allowed to be changed during prepared statements. `PrepareContext()` can be called through `sql.DB`, `sql.Conn`, and `sql.Tx`.

When creating the object through `sql.DB.PrepareContext()`, the `awsctx.SetReadOnly` value can be given as a context and it will target the connection from the value of `awsctx.SetReadOnly`. If none is supplied, it will connect to the writer.

```go
readOnlyCtx := context.WithValue(context.Background(), awsctx.SetReadOnly, true)
writeCtx := context.WithValue(context.Background(), awsctx.SetReadOnly, false)
var name

db, _ := sql.Open("awssql-mysql", "dsn")

// Run statement on reader instance
stmt, _ := db.PrepareContext(readOnlyCtx, "SELECT name FROM users WHERE id = ?")
stmt.QueryRowContext(context.TODO(),1).Scan(&name) // reader instance
// ... Do something with name
stmt.QueryRowContext(context.TODO(),2).Scan(&name) // reader instance
// ... Do something with name
stmt.Close()

// Run statement on Writer instance
stmt, _ := db.PrepareContext(writeCtx, "INSERT INTO users (name, email) VALUES (?, ?)")
stmt.ExecContext(context.TODO(),"bob", "bob@mail.com") // writer instance
stmt.ExecContext(context.TODO(),"joe", "joe@mail.com") // writer instance
stmt.Close()

db.Close()
```

When creating the object through `sql.Conn.PrepareContext()`, the `awsctx.SetReadOnly` value can be given as context and will target the corresponding instance. This will also change the mode of the `sql.Conn` object itself even after the `sql.Stmt` object is closed. If no value was passed, then the query will run against the current instance that `sql.Conn` is pointing to. That is, if `sql.Conn` is currently executing against the reader instance, then the prepared statement will target the reader instance.

```go
readOnlyCtx := context.WithValue(context.Background(), awsctx.SetReadOnly, true)
writeCtx := context.WithValue(context.Background(), awsctx.SetReadOnly, false)
var name

db, _ := sql.Open("awssql-mysql", "dsn")
conn, _ := sql.Conn(context.TODO())

// Run statement on reader instance
stmt, _ := conn.PrepareContext(readOnlyCtx, "SELECT name FROM users WHERE id = ?")
stmt.QueryRowContext(context.TODO(),1).Scan(&name) // reader instance
// ... Do something with name
stmt.QueryRowContext(context.TODO(),2).Scan(&name) // reader instance
// ... Do something with name
stmt.Close()

// Switch to write, on the context level
conn.ExecContext(writeCtx, "...")

// Prepared statement will run against writer because it was switched in the previous line
stmt, _ := db.PrepareContext(writeCtx, "INSERT INTO users (name, email) VALUES (?, ?)")
stmt.ExecContext(context.TODO(),"bob", "bob@mail.com") // writer instance
stmt.ExecContext(context.TODO(),"joe", "joe@mail.com") // writer instance
stmt.Close()

db.Close()
```

When creating the object through `sql.Tx.PrepareContext()`, The query will automatically run against the instance that `sql.Tx` is connected to. **Do not pass in `awsctx.SetReadOnly` or it will cause an error**.  This is because you cannot switch connections during a transaction. 

```go
var name
db, _ := sql.Open("awssql-mysql", "dsn")

// Set to use ReadOnly from TxOptions
tx, _ := db.BeginTx(context.TODO(), &sql.TxOptions{ReadOnly: true})

// Run statement on reader instance. 
// NOTE: Do not set awsctx.SetReadOnly in the PrepareContext statement
stmt, _ := conn.PrepareContext(context.TODO(), "SELECT name FROM users WHERE id = ?")
stmt.QueryRowContext(context.TODO(),1).Scan(&name) // reader instance
// ... Do something with name
stmt.QueryRowContext(context.TODO(),2).Scan(&name) // reader instance
// ... Do something with name
stmt.Close()
tx.Commit()

// Set to use write only through TxOptions
tx, _ := db.BeginTx(context.TODO(), &sql.TxOptions{ReadOnly: false})

// Prepared statement will run against writer because it was switched in the previous line
stmt, _ := db.PrepareContext(writeCtx, "INSERT INTO users (name, email) VALUES (?, ?)")
stmt.ExecContext(context.TODO(),"bob", "bob@mail.com") // writer instance
stmt.ExecContext(context.TODO(),"joe", "joe@mail.com") // writer instance
stmt.Close()
tx.Commit()

db.Close()
```

#### sql.DB

**It is not recommended to use features of the read/write plugin with `sql.DB`**. In general, running queries in which session states need to be preserved are discouraged when using `sql.DB`. Users do not have any control of the lifetime of the connection object and there is no guarantee if the `sql.DB` pool will open a new connection, or re-use an old connection. 

Users can execute queries through a `sql.DB` object with the Read/Write Splitting Plugin safely in one of two ways:

##### 1. Do not pass in awsctx.SetReadOnly for the duration of `sql.DB`. 

Users can use `sql.DB` as is if they do not pass in `awsctx.SetReadOnly`.

```go
dsn := "host=myhost user=myuser password=mypassword port=5432 plugins=readWriteSplitting"
db, _ := sql.Open("awssql-pgx", dsn)

// context can be anything as long as it doesn't contain awsctx.setReadOnly
db.ExecContext(context.TODO(), "SELECT 1") 
db.QueryContext(context.TODO(), "SELECT @my_var")

...

db.Close()
```

#####  2. Pass in a context for every query

Another alternative is to pass in a context through every query. This will guarantee that the query will run against the desired instance. However, using it this way will not provide the performance improvements that the plugin was intended to provide. 

```go
readOnlyCtx := context.WithValue(context.Background(), awsctx.SetReadOnly, true)
writeCtx := context.WithValue(context.Background(), awsctx.SetReadOnly, false)

dsn := "host=myhost user=myuser password=mypassword port=5432 plugins=readWriteSplitting"
db, _ := sql.Open("awssql-pgx", dsn)

db.ExecContext(readOnlyCtx, "SELECT 1") 
db.QueryContext(writeCtx, "INSERT INTO ...")
```

### Using the Read/Write Splitting Plugin against non-Aurora clusters

The Read/Write Splitting Plugin is not currently supported for non-Aurora clusters.

## Internal Connection Pooling

> [!WARNING]
> If internal connection pools are enabled, database passwords may not be verified with every connection request. The initial connection request for each database instance in the cluster will verify the password, but subsequent requests may return a cached pool connection without re-verifying the password. This behavior is inherent to the nature of connection pools in general and not a bug with the wrapper. `<name-of-ConnectionProvider>.ReleaseResources()` can be called to close all pools and remove all cached pool connections. See [Internal Connection Pool Password Warning Example for Postgres](../../../examples/aws_internal_connection_pool_password_warning_postgresql_example.go) and [Internal Connection Pool Password Warning Example for MySQL](../../../examples/aws_internal_connection_pool_password_warning_mysql_example.go)

When `awsctx.setReadOnly=true` is first passed in, the read/write plugin will internally open a new physical connection to a reader. After this first call, the physical reader connection will be cached. Future calls passing `awsctx.setReadOnly=true` object will not require opening a new physical connection, as this was done the first time `awsctx.setReadOnly=true` was passed in.

Due to the abstraction of go's `database/sql` library, many users may often find themselves executing queries with the `sql.DB` object. `sql.DB` is a high-level connection pool, users do not have control of which `sql.Conn` is being used when running queries using methods on this object (such as`db.ExecContext`). That is, if a user were to run `db.ExecContext` multiple times, each of those times could result in creating a new connection object and making a new physical connection to the reader.

If your application uses `sql.DB`, then you can enable internal connection pooling to improve performance. When enabled, the wrapper driver will maintain an internal connection pool for each instance in the cluster. This allows the read/write splitting plugin to reuse connections that were established by queries using `awsctx.setReadOnly=true`.

> [!NOTE]
> Initial connections to a cluster URL will not be pooled. The driver does not pool cluster URLs because it can be problematic to pool a URL that resolves to different instances over time. The main benefit of internal connection pools is when setReadOnly is called. When setReadOnly is called (regardless of the initial connection URL), an internal pool will be created for the writer/reader that the plugin switches to and connections for that instance can be reused in the future.

The wrapper driver can create and maintain a set of internal connection pools using the `InternalPooledConnectionProvider`. The steps to set this up are as follows:

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

5. When you are finished using all connections, call `driver.ClearCaches()`, and then `provider.ReleaseResources()`.

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