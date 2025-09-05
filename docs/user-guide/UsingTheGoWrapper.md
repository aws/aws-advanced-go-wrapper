# Using the AWS Advanced Go Wrapper

The AWS Advanced Go Wrapper leverages community database clients and enables support of AWS RDS and Aurora functionalities.
Currently, the [pgx - PostgreSQL Driver and Toolkit](https://github.com/jackc/pgx) and
the [Go-MySQL-Driver](https://github.com/go-sql-driver/mysql) are supported.

## Using the AWS Advanced Go Wrapper with plain RDS databases

It is possible to use the AWS Advanced Go Wrapper with plain RDS databases, but individual features may or may not be
compatible. For example, failover handling and enhanced failure monitoring are not compatible with plain RDS databases
and the relevant plugins must be disabled. Plugins can be enabled or disabled as seen in
the [Connection Plugin Manager Parameters](#connection-plugin-manager-parameters) section. Please note that some plugins
have been enabled by default. Plugin compatibility can be verified in the [plugins table](#list-of-available-plugins).

## Getting a Connection

The AWS Advanced Go Wrapper is an implementation of Go's database/sql/driver interface. The wrapper contains modules for both the [pgx driver](https://github.com/jackc/pgx) and a [Go-MySQL-Driver](https://github.com/go-sql-driver/mysql). The appropriate driver module must be downloaded along with any additional [plugin](#Plugins) modules. The driver must be imported or imported for side effects to be used.

Use `"awssql-pgx"` or `"awssql-mysql` as `driverName` and a valid DSN as `dataSourceName`.

> [!NOTE]
> The formatting of a valid DSN for a [MySQL](https://github.com/go-sql-driver/mysql#dsn-data-source-name) and [PostgreSQL](https://github.com/jackc/pgx?tab=readme-ov-file#example-usage) connection differs, please provide a valid DSN for the intended underlying driver.

To open a database handle:

```go
import (
	"database/sql"

	// Driver modules can operate separately and do not need to be imported together:
	_ "github.com/aws/aws-advanced-go-wrapper/mysql-driver"
	_ "github.com/aws/aws-advanced-go-wrapper/pgx-driver"
)

func main() {
    pgDsnExample := "host=host port=port user=username dbname=database password=password"
    pgDsnExample2 := "postgres://username:password@host:port/database"
    mySqlDsnExample := "username:password@net(host:port)/database"
    
    db, err := sql.Open("awssql-pgx", pgDsnExample)
    db, err := sql.Open("awssql-mysql", mySqlDsnExample)
    if err != nil {
        panic(err)
    }
    defer db.Close()
}
```

To obtain a connection from a driver:

```go
import (
	"database/sql"

    // Driver modules can operate separately and do not need to be imported together:
    "github.com/aws/aws-advanced-go-wrapper/mysql-driver"
    "github.com/aws/aws-advanced-go-wrapper/pgx-driver"
)

func main() {
    pgDsnExample := "host=host port=port user=username dbname=database password=password"
    pgDsnExample2 := "postgres://username:password@host:port/database"
    mySqlDsnExample := "username:password@net(host:port)/database"

    pgxDriver := &pgx_driver.PgxDriver{}
    mysqlDriver := &mysql_driver.MySQLDriver{}
    
    conn, err := pgxDriver.Open(pgDsnExample)
    if err != nil {
        panic(err)
    }
    defer conn.Close()
}
```

## Logging

To enable logging when using the AWS Advanced Go Wrapper, set the slog `Level` value. The level can be set to one of the
following values: `LevelDebug`, `LevelInfo`, `LevelWarn`, or `LevelError`.

```go
import (
	"log/slog"
)

slog.SetLogLoggerLevel(slog.LevelDebug)
```

## AWS Advanced Go Wrapper Parameters

These parameters are applicable to any instance of the AWS Advanced Go Wrapper.

| Parameter                      | Value    | Required                                                                         | Description                                                                                                                                                                                                                                                                                                                                                                                                                        | Default Value                                                                                                                                                            | Version Supported |
|--------------------------------|----------|----------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------|
| `host`                         | `string` | No                                                                               | Database host.                                                                                                                                                                                                                                                                                                                                                                                                                     | `nil`                                                                                                                                                                    | `latest`          |
| `database`                     | `string` | No                                                                               | Database name.                                                                                                                                                                                                                                                                                                                                                                                                                     | `nil`                                                                                                                                                                    | `latest`          |
| `user`                         | `string` | No                                                                               | Database username.                                                                                                                                                                                                                                                                                                                                                                                                                 | `nil`                                                                                                                                                                    | `latest`          |
| `password`                     | `string` | No                                                                               | Database password.                                                                                                                                                                                                                                                                                                                                                                                                                 | `nil`                                                                                                                                                                    | `latest`          |
| `port`                         | `int`    | No                                                                               | Database port.                                                                                                                                                                                                                                                                                                                                                                                                                     | `nil`                                                                                                                                                                    | `latest`          |
| `protocol`                     | `string` | No                                                                               | The underlying driver protocol AWS Go driver will connect using. Either `postgresql` or `mysql`.                                                                                                                                                                                                                                                                                                                                   | If not provided, determines value from dsn.                                                                                                                              | `latest`          |
| `net`                          | `string` | No. Only used for MySQL.                                                         | The named network to connect with.                                                                                                                                                                                                                                                                                                                                                                                                 | `nil`                                                                                                                                                                    | `latest`          |
| `singleWriterDsn`              | `bool`   | No                                                                               | Set to true if you are providing a dsn with multiple comma-delimited hosts and your cluster has only one writer. The writer must be the first host in the dsn.                                                                                                                                                                                                                                                                     | `false`                                                                                                                                                                  | `latest`          |
| `databaseDialect`              | `string` | No                                                                               | A unique identifier for the supported database dialect. One of `aurora-mysql`, `rds-multi-az-mysql-cluster`, `rds-mysql`, `mysql`, `aurora-pg`, `rds-multi-az-pg-cluster`, `rds-pg`, or `pg`. See [Database Dialects](./DatabaseDialects.md) for details.                                                                                                                                                                          | If not provided, determines value from DSN.                                                                                                                              | `latest`          |
| `targetDriverDialect`          | `string` | No                                                                               | A unique identifier for the target driver dialect. Either `pgx` or `mysql`.                                                                                                                                                                                                                                                                                                                                                        | If not provided, determines value from driver protocol.                                                                                                                  | `latest`          |
| `targetDriverAutoRegister`     | `bool`   | No                                                                               | Allows the AWS Advanced Go Wrapper to auto-register a target driver.                                                                                                                                                                                                                                                                                                                                                               | `true`                                                                                                                                                                   | `latest`          |
| `clusterInstanceHostPattern`   | `string` | If connecting using an IP address or custom domain URL: Yes<br><br>Otherwise: No | This parameter is not required unless connecting to an AWS RDS cluster via an IP address or custom domain URL. In those cases, this parameter specifies the cluster instance DNS pattern that will be used to build a complete instance endpoint. A "?" character in this pattern should be used as a placeholder for the DB instance identifiers of the instances in the cluster. See [here](#host-pattern) for more information. | If the provided host is not an IP address or custom domain, the Go Wrapper will automatically acquire the cluster instance host pattern from the customer-provided host. | `latest`          |
| `transferSessionStateOnSwitch` | `bool`   | No                                                                               | Enables transferring the session state to a new connection.                                                                                                                                                                                                                                                                                                                                                                        | `true`                                                                                                                                                                   | `latest`          |
| `resetSessionStateOnClose`     | `bool`   | No                                                                               | Enables resetting the session state before closing connection.                                                                                                                                                                                                                                                                                                                                                                     | `true`                                                                                                                                                                   | `latest`          |
| `rollbackOnSwitch`             | `bool`   | No                                                                               | Enables rolling back a current transaction, if any in effect, before switching to a new connection.                                                                                                                                                                                                                                                                                                                                | `true`                                                                                                                                                                   | `latest`          |

## Host Pattern

When connecting to Aurora clusters, the [`clusterInstanceHostPattern`](#aws-advanced-go-wrapper-parameters) parameter is
required if the host does not provide enough information about the database cluster domain name.
If the Aurora cluster endpoint is used directly, the AWS Advanced Go Wrapper will recognize the standard Aurora domain
name and can re-build a proper Aurora instance name when needed.
In cases where the host is an IP address, a custom domain name, or localhost, the wrapper won't know how to build a
proper domain name for a database instance endpoint.

For example, if a custom domain was being used and the cluster instance endpoints followed a pattern of
`instanceIdentifier1.customHost`, `instanceIdentifier2.customHost`, etc., the wrapper would need to know how to
construct the instance endpoints using the specified custom domain.
Since there isn't enough information from the custom domain alone to create the instance endpoints, you should set the
`clusterInstanceHostPattern` to `?.customHost`, making the dsn
`"host=host user=user dbname=database password=password clusterInstanceHostPattern=?.customHost"`.

Refer to [this diagram](../images/failover_behavior.png) about AWS Advanced Go Wrapper behavior for different connection
URLs and more details and examples.

## Plugins

The AWS Advanced Go Wrapper uses plugins to execute methods. You can think of a plugin as an extensible code module that
adds extra logic around any database method calls. The AWS Advanced Go Wrapper has a number
of [built-in plugins](#list-of-available-plugins) available for use.

Plugins are loaded and managed through the Connection Plugin Manager and may be identified by a plugin code.

### Connection Plugin Manager Parameters

| Parameter             | Value    | Required | Description                                                                                                                                                 | Default Value  |
|-----------------------|----------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|
| `plugins`             | `string` | No       | Comma separated list of connection plugin codes.                                                                                                            | `failover,efm` |
| `autoSortPluginOrder` | `bool`   | No       | Allows the AWS Advanced Go Wrapper to sort connection plugins to prevent plugin misconfiguration. Allows a user to provide a custom plugin order if needed. | `true`         |

To use a built-in plugin, specify its relevant plugin code in the `plugins` parameter.
The default value for `plugins` is `failover,efm`. These plugins are enabled by default. To read more about these
plugins, see the [List of Available Plugins](#list-of-available-plugins) section.
To override the default plugins, simply provide a new value for `plugins`.
For instance, to use the [IAM Authentication Connection Plugin](./using-plugins/UsingTheIamAuthenticationPlugin.md) and
the [Failover Connection Plugin](./using-plugins/UsingTheFailoverPlugin.md):

```go
db, err := sql.Open("awssql", "host=host user=user dbname=database password=password plugins=iam,failover")
```

> [!NOTE]
> If `autoSortPluginOrder` has been set to false, the plugins will be initialized and executed in the order they have been specified.

Provide the string `"none"` to disable all plugins:

```go
db, err := sql.Open("awssql", "host=host user=user dbname=database password=password plugins=none")
```

The wrapper behaves like the target driver when no plugins are used.

### List of Available Plugins

The AWS Advanced Go Wrapper has several built-in plugins that are available to use. Please visit the individual plugin
page for more details.

| Plugin name                                                                                 | Plugin Code          | Database Compatibility | Description                                                                                                                                                                                                       | Additional Required Dependencies                                                                                               |
|---------------------------------------------------------------------------------------------|----------------------|------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| [Failover Connection Plugin](./using-plugins/UsingTheFailoverPlugin.md)                     | `failover`           | Aurora                 | Enables the failover functionality supported by Amazon Aurora clusters. Prevents opening a wrong connection to an old writer instance due to stale DNS after a failover event. This plugin is enabled by default. | None                                                                                                                           |
| [Host Monitoring Plugin](./using-plugins/UsingTheHostMonitoringPlugin.md)                   | `efm`                | Aurora                 | Enables enhanced host connection failure monitoring, allowing faster failure detection rates. This plugin is enabled by default.                                                                                  | None                                                                                                                           |
| [IAM Authentication Connection Plugin](./using-plugins/UsingTheIamAuthenticationPlugin.md)  | `iam`                | Aurora                 | Enables users to connect to their Amazon Aurora clusters using AWS Identity and Access Management (IAM).                                                                                                          | See the IAM Authentication Connection Plugin [prerequisites](./using-plugins/UsingTheIamAuthenticationPlugin.md#prerequisites) |
| [AWS Secrets Manager Connection Plugin](./using-plugins/UsingTheAwsSecretsManagerPlugin.md) | `awsSecretsManager`  | Any database           | Enables fetching database credentials from the AWS Secrets Manager service.                                                                                                                                       | See the IAM Authentication Connection Plugin [prerequisites](./using-plugins/UsingTheAwsSecretsManagerPlugin.md#prerequisites) |
| [Federated Authentication Plugin](./using-plugins/UsingTheFederatedAuthPlugin.md)           | `federatedAuth`      | Aurora                 | Enables users to authenticate using Federated Identity and then connect to their Amazon Aurora Cluster using AWS Identity and Access Management (IAM).                                                            | See the Federated Authentication Plugin [prerequisites](./using-plugins/UsingTheFederatedAuthPlugin.md#prerequisites)          |
| [Okta Authentication Plugin](./using-plugins/UsingTheOktaAuthPlugin.md)                     | `okta`               | Aurora                 | Enables users to authenticate using Federated Identity and then connect to their Amazon Aurora Cluster using AWS Identity and Access Management (IAM).                                                            | See the Okta Authentication Plugin [prerequisites](./using-plugins/UsingTheOktaAuthPlugin.md#prerequisites)                    |
| [Limitless Connection Plugin](./using-plugins/UsingTheLimitlessConnectionPlugin.md)         | `limitless`          | Aurora                 | Enables client-side load-balancing of Transaction Routers on Amazon Aurora Limitless Databases.                                                                                                                   | None                                                                                                                           |
| [Read Write Splitting Plugin](./using-plugins/UsingTheReadWriteSplittingPlugin.md)          | `readWriteSplitting` | Aurora                 | Enables read write splitting functionality where users can switch between database reader and writer instances.                                                                                                   | None                                                                                                                           |
| [Blue/Green Deployment Plugin](./using-plugins/UsingTheBlueGreenPlugin.md)                  | `bg`                 | Aurora, RDS Instance   | Enables client-side Blue/Green Deployment support.                                                                                                                                                                | None                                                                                                                           |

In addition to the built-in plugins, you can also create custom plugins more suitable for your needs.
For more information, see [Custom Plugins](../contributor-guide/LoadablePlugins.md#using-custom-plugins).
