# Connection Lifetime Plugin
The Connection Lifetime Plugin limits how long the AWS Advanced Go Wrapper uses a physical database connection. When an expired connection is about to make a call that reaches the database server, the plugin either closes the connection or reconnects to the host the application originally supplied.

## Enabling the Connection Lifetime Plugin

To enable the Connection Lifetime Plugin, add `connectionLifetime` to the [`plugins`](../UsingTheGoWrapper.md#connection-plugin-manager-parameters) value. The required `connectionLifetimeMs` parameter must also be set.

```go
db, err := sql.Open("awssql-pgx", "host=host user=user dbname=database password=password plugins=connectionLifetime connectionLifetimeMs=300000")
```

```go
db, err := sql.Open("awssql-mysql", "user:password@tcp(host:3306)/database?plugins=connectionLifetime&connectionLifetimeMs=300000")
```

When plugins are sorted automatically, the Connection Lifetime Plugin runs before every other plugin.

## Connection Lifetime Parameters

| Parameter                  |  Value  | Required | Description                                                                                                                  | Example  | Default Value |
|----------------------------|:-------:|:--------:|------------------------------------------------------------------------------------------------------------------------------|----------|---------------|
| `connectionLifetimeMs`     | Integer |   Yes    | Maximum physical connection lifetime in milliseconds. The value must be at least `1`. Any other value fails the connection.  | `300000` | None          |
| `connectionLifetimeAction` | String  |    No    | Action taken after expiration. Accepted values are `reconnect` and `close`, ignoring letter case. Any other value fails the connection. | `close`  | `reconnect`   |

## Expiration Checks

The lifetime starts when a physical connection is successfully created. A connection that the plugin did not see being created, for example one opened before the plugin was involved, is timed from the moment the plugin first notices it. A connection is expired when its elapsed lifetime is greater than or equal to `connectionLifetimeMs`. The boundary is inclusive, so a connection with a configured lifetime of 1000 ms is expired at exactly 1000 ms.

The plugin checks the lifetime immediately before calls that reach the database server: `Conn.QueryContext`, `Conn.ExecContext`, `Conn.Prepare`, `Conn.PrepareContext`, `Conn.Ping`, `Conn.ResetSession`, `Conn.Begin`, `Conn.BeginTx`, the `Exec` and `Query` methods of prepared statements, `Tx.Commit`, `Tx.Rollback`, and the `Next`, `HasNextResultSet` and `NextResultSet` methods of rows. It does not run a background timer, so an expired connection remains open until the next of these calls triggers the configured action.

Other calls never trigger an action. These include `Conn.Close`, `Conn.IsValid`, `Conn.CheckNamedValue`, `Stmt.Close`, `Stmt.NumInput`, `Rows.Close` and `Rows.Columns`.

The plugin applies only to the connection used by the application. Internal connections opened by other plugins, such as monitoring or topology-refresh connections, have their own lifecycle and are not tracked.

The configured action runs before the pending call. The pending call is not executed when the plugin closes or reconnects the connection.

## Close Action

With `connectionLifetimeAction=close`, the plugin closes the current physical connection before the pending call. It then returns an `AwsWrapperError` of type `ConnectionLifetimeClosedErrorType`, which reports that the connection reached its maximum lifetime and is closed. The error wraps `driver.ErrBadConn`, and any error returned while closing the physical connection, so both can be matched with `errors.Is`.

The connection remains permanently unusable, and every later call that reaches the server returns the same error. Dispose of the connection rather than retrying with it.

## Reconnect Action

With the default `connectionLifetimeAction=reconnect`, the plugin opens a replacement physical connection to the host and port the application supplied in the DSN. It does not use a host selected later by routing, read/write splitting, the initial connection strategy, or failover. The replacement is opened through the rest of the plugin chain, so authentication plugins such as IAM still apply. The wrapper installs the replacement in the existing connection and resets the lifetime only after installation succeeds.

After a successful reconnect, the plugin returns `error_util.ConnectionLifetimeSuccessError`. This error reports that the existing connection now holds a usable replacement physical connection. The pending call was not executed. Restore any required session state and retry the operation when appropriate.

`ConnectionLifetimeSuccessError` has the error type `FailoverSuccessErrorType`, so code that already handles a [`FailoverSuccessError`](./UsingTheFailoverPlugin.md#failoversuccesserror) with `error_util.IsType(err, error_util.FailoverSuccessErrorType)` also handles a reconnect. Use `errors.Is` with `error_util.ConnectionLifetimeSuccessError` to tell the two apart.

```go
import (
	"errors"

	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/error_util"
)

_, err := conn.ExecContext(ctx, query)
if errors.Is(err, error_util.ConnectionLifetimeSuccessError) {
	// The physical connection was replaced and the statement was not executed.
	// Restore session state if required, then retry.
	_, err = conn.ExecContext(ctx, query)
}
```

Prepared statements and rows created before reconnection belong to the previous physical connection and may be stale. Close them and create new ones from the connection before retrying.

If opening or installing the replacement fails, the plugin returns an `AwsWrapperError` of type `ConnectionLifetimeFailedErrorType`. The error wraps `driver.ErrBadConn` together with the reconnect error, so the cause can be matched with `errors.Is` or `errors.As`. Dispose of the connection.

A successful reconnect starts a new lifetime interval. A failed reconnect does not reset the timer. If another plugin switches the physical connection, the plugin tracks the new physical connection separately, and a connection that is switched back to keeps its original creation time.

## Transactions

Neither action runs while a transaction is active. If the lifetime expires during a transaction, calls continue on the current physical connection. This allows `Tx.Commit` and `Tx.Rollback` to execute without closing or reconnecting first. Deferral does not extend or reset the lifetime.

After the transaction ends, the first eligible call runs the configured action before that call: `close` closes the connection and returns the closed error; `reconnect` reconnects and returns `ConnectionLifetimeSuccessError`. The pending call is not executed in either case.

## Internal Connection Pools

With [internal connection pooling](./UsingTheReadWriteSplittingPlugin.md#internal-connection-pooling), closing a connection only returns its physical connection to the internal pool. The plugin therefore also evicts an expired connection from the internal pool, so the pool closes the physical connection instead of handing it out again. A connection that the Read/Write Splitting Plugin keeps open for reuse is not evicted.

A replacement connection taken from the internal pool can already be older than `connectionLifetimeMs`, because the plugin times a connection from when it receives it. To also limit the age of pooled physical connections, set `internal_pool.WithMaxConnLifetime` in the pool options.

A custom connection provider that keeps its own pool can implement `driver_infrastructure.ConnectionEvictor` to have expired connections evicted the same way.

## Using the Plugin with database/sql

The errors are designed to work with the connection pool in `database/sql`:

- `database/sql` calls `Conn.ResetSession` before it reuses a pooled connection, so an idle connection that expired is usually handled there. With `reconnect`, the replacement is installed and `database/sql` hands out the connection as usual. With `close`, the error wraps `driver.ErrBadConn`, so `database/sql` discards the connection and uses another one.
- When the closed error or the reconnect failure error is returned, `database/sql` discards the connection because the error wraps `driver.ErrBadConn`. For calls made on `sql.DB` or on a statement prepared from it, `database/sql` also retries the call on another connection. The retry is safe because the plugin did not send the pending call. Calls made on an `sql.Conn` or on `sql.Rows` return the error to the application instead.
- `ConnectionLifetimeSuccessError` returned from a query is passed to the application, which should handle it as shown in [Reconnect Action](#reconnect-action).

`sql.DB.SetConnMaxLifetime` closes pooled connections that are older than a limit when they are returned to or taken from the pool. The Connection Lifetime Plugin also covers connections that stay checked out for a long time, such as an `sql.Conn` or a `driver.Conn` used directly, and can replace the physical connection without discarding the wrapper connection.
