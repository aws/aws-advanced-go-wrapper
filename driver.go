package driver

import (
	"database/sql"
	"database/sql/driver"
	"github.com/go-sql-driver/mysql"

	_ "github.com/go-sql-driver/mysql"
)

type AwsWrapperDriver struct {
	targetDriver  driver.Driver
	pluginManager PluginManager
}

func (d AwsWrapperDriver) Open(dsn string) (driver.Conn, error) {
	// do wrapper driver things
	d.pluginManager.Init()

	// call underlying driver
	conn, err := d.targetDriver.Open(dsn)
	return AwsWrapperConn{underlyingConn: conn, pluginManager: d.pluginManager}, err
}

var driverName = "aws"

func init() {
	if driverName != "" {
		var targetDriver = &mysql.MySQLDriver{}
		sql.Register(
			driverName,
			&AwsWrapperDriver{
				targetDriver,
				&ConnectionPluginManager{targetDriver: targetDriver}})
	}
}

type AwsWrapperConn struct {
	underlyingConn driver.Conn
	pluginManager  PluginManager
}

func (c AwsWrapperConn) Prepare(query string) (driver.Stmt, error) {
	// do wrapper driver things

	// call underlying driver
	//return c.underlyingConn.Prepare(query)
	prepareFunc := func() (driver.Stmt, error) { return c.underlyingConn.Prepare(query) }
	return c.pluginManager.Prepare(prepareFunc)
}

func (c AwsWrapperConn) Close() error {
	// close wrapper driver things

	// close underlying driver
	return c.underlyingConn.Close()
}

func (c AwsWrapperConn) Begin() (driver.Tx, error) {
	// do wrapper driver things

	// call underlying driver
	return c.underlyingConn.Begin() //nolint:all
}
