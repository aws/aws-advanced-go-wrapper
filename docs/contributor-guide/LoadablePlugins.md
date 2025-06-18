# Plugins As Loadable Modules

Plugins are loadable and extensible modules that add extra logic around Go sql method calls.

Plugins let users:

- monitor connections
- handle errors during executions
- log execution details, such as SQL statements executed
- cache execution results
- measure execution time
- and more

The AWS Advanced Go Wrapper has several built-in plugins; you
can [see the list here](../user-guide/UsingTheGoWrapper.md#list-of-available-plugins).

## Available Services

Plugins are notified by the plugin manager when changes to the database connection occur, and utilize
the [plugin service](./PluginService.md) to establish connections and retrieve host information.

## Using Custom Plugins

To use a custom plugin, you must:

1. Create a custom plugin and plugin factory.
2. Add a code and the plugin factory to the map of available plugins.
3. Specify the custom plugin by including the added code in the `plugins` parameter.

### Creating Custom Plugins

There are two ways to create a custom plugin:

- implement the [ConnectionPlugin](../../awssql/driver_infrastructure/connection_plugin.go) interface directly, or
- extend the [BaseConnectionPlugin](../../awssql/plugins/base_connection_plugin.go) class.

The `BaseConnectionPlugin` struct provides a simple implementation for all the methods in `ConnectionPlugin`,
as it calls the provided Go sql method without additional operations. This is helpful when the custom plugin only needs
to override one (or a few) methods from the `ConnectionPlugin` interface.
See the following classes for examples:

- [IamAuthPlugin](../../iam/iam_auth_plugin.go)
    - The `IamAuthPlugin` class only overrides the `connect` method because the plugin is only concerned with creating
      database connections with IAM database credentials.

- [ExecutionTimePlugin](../../awssql/plugins/execution_time_plugin.go)
    - The `ExecutionTimePlugin` only overrides the `execute` method because it is only concerned with elapsed time
      during execution, it does not establish new connections or set up any host list provider.

A `ConnectionPluginFactory` implementation is also required for the new custom plugin. This factory class is used to
register and initialize custom plugins. See [ExecutionTimePluginFactory](../../awssql/plugins/execution_time_plugin.go)
for a simple implementation example.

### Subscribed Methods

The `GetSubscribedMethods() []string` method specifies a set of Go sql methods a plugin is subscribed to. All plugins
must implement the `GetSubscribedMethods() []string` method.

When executing a Go sql method, the plugin manager will only call a specific plugin method if the Go sql method is
within its set of subscribed methods.

Plugins can subscribe to any of the Go sql methods listed [here](https://pkg.go.dev/database/sql#pkg-functions) for
Conn, Result, Rows, Stmt, and Tx; some examples are as follows:

- `Conn.ExecContext`
- `Stmt.ExecContext`
- `Rows.Next`

Plugins can also subscribe to the following pipelines:

| Pipeline                                                                                            | Method Name / Subscription Key |
|-----------------------------------------------------------------------------------------------------|:------------------------------:|
| [Host list provider pipeline](./Pipelines.md#host-list-provider-pipeline)                           |        initHostProvider        |
| [Connect pipeline](./Pipelines.md#connect-pipeline)                                                 |          Conn.Connect          |
| [Connection changed notification pipeline](./Pipelines.md#connection-changed-notification-pipeline) |    notifyConnectionChanged     |
| [Host list changed notification pipeline](./Pipelines.md#host-list-changed-notification-pipeline)   |     notifyHostListChanged      |                                                                      

### Tips on Creating a Custom Plugin

A custom plugin can subscribe to all Go sql methods being executed, which means it may be active in every workflow.
We recommend that you be aware of the performance impact of subscribing and performing demanding tasks for every
method.

### Add the Custom Plugin to Available Plugins

The AwsWrapperDriver is used when opening connections to the DB. Custom plugins can be added by including their plugin
factory in the map of available factories.

```go
import (
    awssql "github.com/aws/aws-advanced-go-wrapper/awssql/driver"
)

func main() {
    awssql.UsePluginFactory("foo", NewFooPluginFactory())
    // Open a connection, the code "foo" can now be added to the plugins parameter to include the FooPlugin.
}
```

## What is Not Allowed in Plugins

When creating custom plugins, it is important to **avoid** the following bad practices in your plugin implementation:

1. Keeping local copies of shared information:
    - information like current connection, or the host list provider are shared across all plugins
    - shared information may be updated by any plugin at any time and should be retrieved via the plugin service when
      required
2. Using driver-specific properties or objects:
    - the AWS Advanced Go Wrapper may be used with multiple drivers, therefore plugins must ensure implementation is not
      restricted to a specific driver
3. Making direct connections:
    - the plugin should always call the pipeline lambdas (i.e. `ConnectFunc func() (driver.Conn, error)`)

See the following examples for more details:

<details><summary>❌ <strong>Bad Example</strong></summary>

```go
type BadExample struct {
    PluginService       driver_infrastructure.PluginService
    // Bad Practice #1: keeping local copies of items
    // Plugins should not keep local copies of the host list provider, the topology or the connection.
    // Host list provider is kept in the Plugin Service and can be modified by other plugins,
    // therefore it should be retrieved by calling pluginService.getHostListProvider() when it is needed.
    hostListProvider    driver_infrastructure.HostListProvider
    props               map[string]string
}

func (b *BadExample) GetSubscribedMethods() []string {
    return []string{plugin_helpers.ALL_METHODS}
}


func (b *BadExample) Connect(
    hostInfo *host_info_util.HostInfo,
    props map[string]string,
    isInitialConnection bool,
    connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
        // Bad Practice #2: using driver-specific objects.
        // Not all drivers support the same configuration parameters. For instance, while go-sql supports "readTimeout",
        // pgx does not.
        props["readTimeout"] = "30s"
        // Bad Practice #3: Making direct connections, should use connectFunc().
        dsn := constructDsnFromProps(props)
        return &mysql.MySQLDriver{}.Open(dsn)
}
```

</details>

<details><summary>✅ <strong>Good Example</strong></summary>

```jgo
type GoodExample struct {
    pluginService       driver_infrastructure.PluginService
    props               map[string]string
}

func (g *GoodExample) GetSubscribedMethods() []string {
    return []string{plugin_helpers.ALL_METHODS}
}

func (g *GoodExample) Connect(
    hostInfo *host_info_util.HostInfo,
    props map[string]string,
    isInitialConnection bool,
    connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
        if property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.USER) == "replace" {
            props[property_util.USER.Name] = "new value"
        }
        return connectFunc()
}

func (g *GoodExample) Execute(
    connInvokedOn driver.Conn,
    methodName string,
    executeFunc driver_infrastructure.ExecuteFunc,
    methodArgs ...any) (wrappedReturnValue any, wrappedReturnValue2 any, wrappedOk bool, wrappedErr error) {
        if len(g.pluginService.GetHosts()) == 0 {
            // Re-fetch host information if it is empty.
            g.pluginService.ForceRefreshHostList(g.pluginService.GetCurrentConnection());
        }
        return executeFunc()
}
```

</details>

