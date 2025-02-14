/*
  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

  Licensed under the Apache License, Version 2.0 (the "License").
  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/stdlib"
)

type AwsWrapperDriver struct {
	targetDriver  driver.Driver
	pluginManager PluginManager
}

func (d *AwsWrapperDriver) Open(dsn string) (driver.Conn, error) {
	// Set up plugin manager.
	d.pluginManager.Init()

	// Call underlying driver and wrap connection.
	conn, err := d.targetDriver.Open(dsn)
	return &AwsWrapperConn{underlyingConn: conn, pluginManager: d.pluginManager}, err
}

// TODO: remove hard coding of underlying drivers.
// See ticket: "dev: address hardcoded underlying driver".
func init() {
	var targetDriver = &mysql.MySQLDriver{}
	sql.Register(
		"aws-mysql",
		&AwsWrapperDriver{
			targetDriver,
			&ConnectionPluginManager{targetDriver: targetDriver}})
	var otherDriver = &stdlib.Driver{}
	sql.Register(
		"aws-pgx",
		&AwsWrapperDriver{
			otherDriver,
			&ConnectionPluginManager{targetDriver: targetDriver}})
}

type AwsWrapperConn struct {
	underlyingConn driver.Conn
	pluginManager  PluginManager
}

func (c *AwsWrapperConn) Prepare(query string) (driver.Stmt, error) {
	prepareFunc := func() (any, error) { return c.underlyingConn.Prepare(query) }
	return prepareWithPlugins(c.pluginManager, "Conn.Prepare", prepareFunc)
}

func (c *AwsWrapperConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	prepareCtx, ok := c.underlyingConn.(driver.ConnPrepareContext)
	if !ok {
		return nil, errors.New(GetMessage("AwsWrapperConn.underlyingConnDoesNotImplementRequiredInterface", "driver.ConnPrepareContext"))
	}
	prepareFunc := func() (any, error) { return prepareCtx.PrepareContext(ctx, query) }
	return prepareWithPlugins(c.pluginManager, "Conn.PrepareContext", prepareFunc)
}

func (c *AwsWrapperConn) Close() error {
	return c.underlyingConn.Close()
}

func (c *AwsWrapperConn) Begin() (driver.Tx, error) {
	beginFunc := func() (any, error) { return c.underlyingConn.Begin() } //nolint:all
	return beginWithPlugins(c.pluginManager, "Conn.Begin", beginFunc)
}

func (c *AwsWrapperConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	beginTx, ok := c.underlyingConn.(driver.ConnBeginTx)
	if !ok {
		return nil, errors.New(GetMessage("AwsWrapperConn.underlyingConnDoesNotImplementRequiredInterface", "driver.ConnBeginTx"))
	}
	beginFunc := func() (any, error) { return beginTx.BeginTx(ctx, opts) }
	return beginWithPlugins(c.pluginManager, "Conn.BeginTx", beginFunc)
}

func (c *AwsWrapperConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	queryerCtx, ok := c.underlyingConn.(driver.QueryerContext)
	if !ok {
		return nil, errors.New(GetMessage("AwsWrapperConn.underlyingConnDoesNotImplementRequiredInterface", "driver.QueryerContext"))
	}
	queryFunc := func() (any, error) { return queryerCtx.QueryContext(ctx, query, args) }
	return queryWithPlugins(c.pluginManager, "Conn.QueryContext", queryFunc)
}

func (c *AwsWrapperConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	execerCtx, ok := c.underlyingConn.(driver.ExecerContext)
	if !ok {
		return nil, errors.New(GetMessage("AwsWrapperConn.underlyingConnDoesNotImplementRequiredInterface", "driver.ExecerContext"))
	}
	execFunc := func() (any, error) { return execerCtx.ExecContext(ctx, query, args) }
	return execWithPlugins(c.pluginManager, "Conn.ExecContext", execFunc)
}

func (c *AwsWrapperConn) Ping(ctx context.Context) error {
	pinger, ok := c.underlyingConn.(driver.Pinger)
	if !ok {
		return errors.New(GetMessage("AwsWrapperConn.underlyingConnDoesNotImplementRequiredInterface", "driver.Pinger"))
	}
	pingFunc := func() (any, error) { return nil, pinger.Ping(ctx) }
	_, err := executeWithPlugins(c.pluginManager, "Conn.Ping", pingFunc)
	return err
}

func (c *AwsWrapperConn) IsValid() bool {
	validator, ok := c.underlyingConn.(driver.Validator)
	if ok {
		return validator.IsValid()
	}
	return true
}

func (c *AwsWrapperConn) ResetSession(ctx context.Context) error {
	resetter, ok := c.underlyingConn.(driver.SessionResetter)
	if !ok {
		return errors.New(GetMessage("AwsWrapperConn.underlyingConnDoesNotImplementRequiredInterface", "driver.SessionResetter"))
	}
	resetSessionFunc := func() (any, error) { return nil, resetter.ResetSession(ctx) }
	_, err := executeWithPlugins(c.pluginManager, "Conn.ResetSession", resetSessionFunc)
	return err
}

type AwsWrapperStmt struct {
	underlyingStmt driver.Stmt
	pluginManager  PluginManager
}

func (a *AwsWrapperStmt) Close() error {
	return a.underlyingStmt.Close()
}

func (a *AwsWrapperStmt) Exec(args []driver.Value) (driver.Result, error) {
	execFunc := func() (any, error) { return a.underlyingStmt.Exec(args) } //nolint:all
	return execWithPlugins(a.pluginManager, "Stmt.Exec", execFunc)
}

func (a *AwsWrapperStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	execerCtx, ok := a.underlyingStmt.(driver.StmtExecContext)
	if !ok {
		return nil, errors.New(GetMessage("AwsWrapperStmt.underlyingStmtDoesNotImplementRequiredInterface", "driver.StmtExecContext"))
	}
	execFunc := func() (any, error) { return execerCtx.ExecContext(ctx, args) }
	return execWithPlugins(a.pluginManager, "Stmt.ExecContext", execFunc)
}

func (a *AwsWrapperStmt) NumInput() int {
	numInputFunc := func() (any, error) { return a.underlyingStmt.NumInput(), nil }
	result, _ := executeWithPlugins(a.pluginManager, "Stmt.NumInput", numInputFunc)
	num, ok := result.(int)
	if ok {
		return num
	}
	return -1
}

func (a *AwsWrapperStmt) Query(args []driver.Value) (driver.Rows, error) {
	queryFunc := func() (any, error) { return a.underlyingStmt.Query(args) } //nolint:all
	return queryWithPlugins(a.pluginManager, "Stmt.Query", queryFunc)
}

func (a *AwsWrapperStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	queryerCtx, ok := a.underlyingStmt.(driver.StmtQueryContext)
	if !ok {
		return nil, errors.New(GetMessage("AwsWrapperStmt.underlyingStmtDoesNotImplementRequiredInterface", "driver.StmtQueryContext"))
	}
	queryFunc := func() (any, error) { return queryerCtx.QueryContext(ctx, args) }
	return queryWithPlugins(a.pluginManager, "Stmt.QueryContext", queryFunc)
}
