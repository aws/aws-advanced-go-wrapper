package driver

import (
	"database/sql/driver"
	"fmt"
)

type OpenFunc func() (driver.Conn, error)

type PrepareFunc func() (driver.Stmt, error)

type ConnectionPlugin interface {
	Open(dsn string, openFunc OpenFunc) (driver.Conn, error)
	Prepare(prepareFunc PrepareFunc) (driver.Stmt, error)
}

type DummyPlugin struct {
	id int
}

func (p DummyPlugin) Open(dsn string, openFunc OpenFunc) (driver.Conn, error) {
	fmt.Println("DummyPlugin.Open()")
	return openFunc()
}

func (p DummyPlugin) Prepare(prepareFunc PrepareFunc) (driver.Stmt, error) {
	fmt.Printf("DummyPlugin.Prepare():%d\n", p.id)
	return prepareFunc()
}
