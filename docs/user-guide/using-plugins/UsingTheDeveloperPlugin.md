## Developer Plugin

> [!WARNING]
> The plugin is NOT intended to be used in production environments. It's designed for the purpose of testing.

The Developer Plugin allows developers to inject an error to a connection and to verify how an application handles it.

Since some errors raised by the drivers rarely happen, testing for those might be difficult and require a lot of effort in building a testing environment. Errors associated with network outages are a good example of those errors. It may require substantial efforts to design and build a testing environment where such timeout errors could be produced with 100% accuracy and 100% guarantee. If a test suite can't produce and verify such cases with 100% accuracy it significantly decreases the value of such tests and makes the tests unstable and flaky. The Developer Plugin simplifies testing of such scenarios as shown below.

The `dev` plugin code should be added to the `plugins` parameter in order to be able to intercept calls and raise a test error when conditions are met.

### Simulate an error while opening a new connection

The Developer Plugin introduces the `ErrorSimulationManager` that will handle error simulation on connection.

In order to raise a test error while opening a new connection, retrieve the driver's ErrorSimulationManager instance by calling the `error_simulator.GetErrorSimulatorManager()` function, then use `RaiseErrorOnNextConnect` in `ErrorSimulationManager` so it will be triggered at next connection attempt.

Once the error is returned, it will be cleared and will not be returned again. This means that the next opened connection will not return the error again.

```go
host := "host"
port := "5432"
user := "user"
password := "password"
dbName := "db"
plugins := "dev"

connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s plugins=%s",
    host, port, user, password, dbName, plugins,
)
db, err := sql.Open("awssql-pgx", connStr)

errSimManager := error_simulator.GetErrorSimulatorManager()
customError := errors.New("test");
errSimManager.RaiseErrorOnNextConnect(testErrorToRaise);

conn1, err := db.Conn(context.TODO()) // err will be customErr

conn2, err := db.Conn(context.TODO()) // err will be nil
```

### Simulate an error with already opened connection

It is possible to also simulate an error thrown in a connection after the connection has been opened.

First, create the sql.Conn object, and then use `error_simulator_util.GetErrorSimulatorFromSqlConn(conn *sql.Conn)` function to retrieve an instance of `ErrorSimulator` that corresponds to that connection object.

Similar to previous case, the error is cleared up once it's raised and subsequent calls should behave normally.

```go
host := "host"
port := "5432"
user := "user"
password := "password"
dbName := "db"
plugins := "dev"

connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s plugins=%s",
    host, port, user, password, dbName, plugins,
)
db, _ := sql.Open("awssql-pgx", connStr)
conn, _ := db.Conn(context.TODO())

errSim := error_simulator.GetConnErrorSimulatorFromSqlConn(conn)

customError := errors.New("test");
errSim.RaiseErrorOnNextCall(customError, "Conn.QueryContext")

err = conn.QueryRowContext(context.TODO(), "SELECT 1").Scan(&result) // err will be customErr
err = conn.QueryRowContext(context.TODO(), "SELECT 1").Scan(&result) // err will be nil
```

It's possible to use a callback function to check call parameters and decide whether to return an error or not. Please see the sample code below for more details.

#### Sample Code

[PostgreSQL Developer Plugin Sample Code](../../../examples/dev_plugin_postgres_example.go)<br>
[MySQL Developer Plugin Sample Code](../../../examples/dev_plugin_mysql_example.go)<br>
