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
	"awssql/container"
	"awssql/driver_infrastructure"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/stdlib"
)

type DatabaseEngine string

const (
	MYSQL DatabaseEngine = "mysql"
	PG    DatabaseEngine = "pg"
)

type AwsWrapperDriver struct {
	targetDriver driver.Driver
	engine       DatabaseEngine
}

func (d *AwsWrapperDriver) Open(dsn string) (driver.Conn, error) {
	// Call underlying driver_infrastructure and wrap connection.
	conn, err := d.targetDriver.Open(dsn)
	if err != nil {
		return nil, err
	}

	wrapperContainer, err := container.NewContainer(dsn, d.targetDriver)
	if err != nil || wrapperContainer.PluginService == nil || wrapperContainer.PluginManager == nil {
		return nil, err
	}

	return NewAwsWrapperConn(conn, wrapperContainer, d.engine), err
}

// TODO: remove hard coding of underlying drivers.
// See ticket: "dev: address hardcoded underlying driver".
func init() {
	var targetDriver = &mysql.MySQLDriver{}
	sql.Register(
		"aws-mysql",
		&AwsWrapperDriver{
			targetDriver: targetDriver,
			engine:       MYSQL})
	var otherDriver = &stdlib.Driver{}
	sql.Register(
		"aws-pgx",
		&AwsWrapperDriver{
			targetDriver: otherDriver,
			engine:       PG})
}

type AwsWrapperConn struct {
	underlyingConn driver.Conn
	container      container.Container
	pluginManager  driver_infrastructure.PluginManager
	engine         DatabaseEngine
}

func NewAwsWrapperConn(underlyingConn driver.Conn, container container.Container, engine DatabaseEngine) *AwsWrapperConn {
	return &AwsWrapperConn{underlyingConn, container, *container.PluginManager, engine}
}

func (c *AwsWrapperConn) Prepare(query string) (driver.Stmt, error) {
	prepareFunc := func() (any, any, bool, error) {
		result, err := c.underlyingConn.Prepare(query)
		return result, nil, false, err
	}
	return prepareWithPlugins(c.pluginManager, "Conn.Prepare", prepareFunc, *c)
}

func (c *AwsWrapperConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	prepareCtx, ok := c.underlyingConn.(driver.ConnPrepareContext)
	if !ok {
		return nil, errors.New(driver_infrastructure.GetMessage("AwsWrapperConn.underlyingConnDoesNotImplementRequiredInterface", "driver.ConnPrepareContext"))
	}
	prepareFunc := func() (any, any, bool, error) {
		result, err := prepareCtx.PrepareContext(ctx, query)
		return result, nil, false, err
	}
	return prepareWithPlugins(c.pluginManager, "Conn.PrepareContext", prepareFunc, *c)
}

func (c *AwsWrapperConn) Close() error {
	closeFunc := func() (any, any, bool, error) { return nil, nil, false, c.underlyingConn.Close() }
	_, _, _, err := ExecuteWithPlugins(c.pluginManager, "Conn.Close", closeFunc)
	return err
}

func (c *AwsWrapperConn) Begin() (driver.Tx, error) {
	beginFunc := func() (any, any, bool, error) {
		result, err := c.underlyingConn.Begin() //nolint:all
		return result, nil, false, err
	}
	return beginWithPlugins(c.pluginManager, "Conn.Begin", beginFunc)
}

func (c *AwsWrapperConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	beginTx, ok := c.underlyingConn.(driver.ConnBeginTx)
	if !ok {
		return nil, errors.New(driver_infrastructure.GetMessage("AwsWrapperConn.underlyingConnDoesNotImplementRequiredInterface", "driver.ConnBeginTx"))
	}
	beginFunc := func() (any, any, bool, error) {
		result, err := beginTx.BeginTx(ctx, opts)
		return result, nil, false, err
	}
	return beginWithPlugins(c.pluginManager, "Conn.BeginTx", beginFunc)
}

func (c *AwsWrapperConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	queryerCtx, ok := c.underlyingConn.(driver.QueryerContext)
	if !ok {
		return nil, errors.New(driver_infrastructure.GetMessage("AwsWrapperConn.underlyingConnDoesNotImplementRequiredInterface", "driver.QueryerContext"))
	}
	queryFunc := func() (any, any, bool, error) {
		result, err := queryerCtx.QueryContext(ctx, query, args)
		return result, nil, false, err
	}
	return queryWithPlugins(c.pluginManager, "Conn.QueryContext", queryFunc, c.engine)
}

func (c *AwsWrapperConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	execerCtx, ok := c.underlyingConn.(driver.ExecerContext)
	if !ok {
		return nil, errors.New(driver_infrastructure.GetMessage("AwsWrapperConn.underlyingConnDoesNotImplementRequiredInterface", "driver.ExecerContext"))
	}
	execFunc := func() (any, any, bool, error) {
		result, err := execerCtx.ExecContext(ctx, query, args)
		return result, nil, false, err
	}
	return execWithPlugins(c.pluginManager, "Conn.ExecContext", execFunc)
}

func (c *AwsWrapperConn) Ping(ctx context.Context) error {
	pinger, ok := c.underlyingConn.(driver.Pinger)
	if !ok {
		return errors.New(driver_infrastructure.GetMessage("AwsWrapperConn.underlyingConnDoesNotImplementRequiredInterface", "driver.Pinger"))
	}
	pingFunc := func() (any, any, bool, error) { return nil, nil, false, pinger.Ping(ctx) }
	_, _, _, err := ExecuteWithPlugins(c.pluginManager, "Conn.Ping", pingFunc)
	return err
}

func (c *AwsWrapperConn) IsValid() bool {
	validator, ok := c.underlyingConn.(driver.Validator)
	if ok {
		isValidFunc := func() (any, any, bool, error) { return nil, nil, validator.IsValid(), nil }
		_, _, result, _ := ExecuteWithPlugins(c.pluginManager, "Conn.Ping", isValidFunc)
		return result
	}
	return true
}

func (c *AwsWrapperConn) ResetSession(ctx context.Context) error {
	resetter, ok := c.underlyingConn.(driver.SessionResetter)
	if !ok {
		return errors.New(driver_infrastructure.GetMessage("AwsWrapperConn.underlyingConnDoesNotImplementRequiredInterface", "driver.SessionResetter"))
	}
	resetSessionFunc := func() (any, any, bool, error) { return nil, nil, false, resetter.ResetSession(ctx) }
	_, _, _, err := ExecuteWithPlugins(c.pluginManager, "Conn.ResetSession", resetSessionFunc)
	return err
}

func (c *AwsWrapperConn) CheckNamedValue(val *driver.NamedValue) error {
	namedValueChecker, ok := c.underlyingConn.(driver.NamedValueChecker)
	if !ok {
		return errors.New(driver_infrastructure.GetMessage("AwsWrapperConn.underlyingConnDoesNotImplementRequiredInterface", "driver.NamedValueChecker"))
	}

	checkNamedValueFunc := func() (any, any, bool, error) { return nil, nil, false, namedValueChecker.CheckNamedValue(val) }
	_, _, _, err := ExecuteWithPlugins(c.pluginManager, "Conn.CheckNamedValue", checkNamedValueFunc)
	return err
}

type AwsWrapperStmt struct {
	underlyingStmt driver.Stmt
	pluginManager  driver_infrastructure.PluginManager
	conn           AwsWrapperConn
}

func (a *AwsWrapperStmt) Close() error {
	closeFunc := func() (any, any, bool, error) { return nil, nil, false, a.underlyingStmt.Close() }
	_, _, _, err := ExecuteWithPlugins(a.pluginManager, "Stmt.Close", closeFunc)
	return err
}

func (a *AwsWrapperStmt) Exec(args []driver.Value) (driver.Result, error) {
	execFunc := func() (any, any, bool, error) {
		result, err := a.underlyingStmt.Exec(args) //nolint:all
		return result, nil, false, err
	}
	return execWithPlugins(a.pluginManager, "Stmt.Exec", execFunc)
}

func (a *AwsWrapperStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	execerCtx, ok := a.underlyingStmt.(driver.StmtExecContext)
	if !ok {
		return nil, errors.New(driver_infrastructure.GetMessage("AwsWrapperStmt.underlyingStmtDoesNotImplementRequiredInterface", "driver.StmtExecContext"))
	}
	execFunc := func() (any, any, bool, error) {
		result, err := execerCtx.ExecContext(ctx, args)
		return result, nil, false, err
	}
	return execWithPlugins(a.pluginManager, "Stmt.ExecContext", execFunc)
}

func (a *AwsWrapperStmt) NumInput() int {
	numInputFunc := func() (any, any, bool, error) { return a.underlyingStmt.NumInput(), nil, false, nil }
	result, _, _, _ := ExecuteWithPlugins(a.pluginManager, "Stmt.NumInput", numInputFunc)
	num, ok := result.(int)
	if ok {
		return num
	}
	return -1
}

func (a *AwsWrapperStmt) Query(args []driver.Value) (driver.Rows, error) {
	queryFunc := func() (any, any, bool, error) {
		result, err := a.underlyingStmt.Query(args) //nolint:all
		return result, nil, false, err
	}
	return queryWithPlugins(a.pluginManager, "Stmt.Query", queryFunc, a.conn.engine)
}

func (a *AwsWrapperStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	queryerCtx, ok := a.underlyingStmt.(driver.StmtQueryContext)
	if !ok {
		return nil, errors.New(driver_infrastructure.GetMessage("AwsWrapperStmt.underlyingStmtDoesNotImplementRequiredInterface", "driver.StmtQueryContext"))
	}
	queryFunc := func() (any, any, bool, error) {
		result, err := queryerCtx.QueryContext(ctx, args)
		return result, nil, false, err
	}
	return queryWithPlugins(a.pluginManager, "Stmt.QueryContext", queryFunc, a.conn.engine)
}

func (a *AwsWrapperStmt) CheckNamedValue(val *driver.NamedValue) error {
	namedValueChecker, ok := a.underlyingStmt.(driver.NamedValueChecker)
	if !ok {
		// If underlyingStmt does not implement the NamedValueChecker interface, fallback to the conn the statement was prepared from.
		return a.conn.CheckNamedValue(val)
	}
	checkNamedValueFunc := func() (any, any, bool, error) { return nil, nil, false, namedValueChecker.CheckNamedValue(val) }
	_, _, _, err := ExecuteWithPlugins(a.pluginManager, "Stmt.CheckNamedValue", checkNamedValueFunc)
	return err
}

type AwsWrapperResult struct {
	underlyingResult driver.Result
	pluginManager    driver_infrastructure.PluginManager
}

func (a *AwsWrapperResult) LastInsertId() (int64, error) {
	execFunc := func() (any, any, bool, error) {
		result, err := a.underlyingResult.LastInsertId()
		return result, nil, false, err
	}
	result, _, _, err := ExecuteWithPlugins(a.pluginManager, "Result.LastInsertId", execFunc)
	if err == nil {
		num, ok := result.(int64)
		if ok {
			return num, nil
		}
		err = errors.New(driver_infrastructure.GetMessage("AwsWrapperExecuteWithPlugins.unableToCastResult", "int64"))
	}
	return -1, err
}

func (a *AwsWrapperResult) RowsAffected() (int64, error) {
	execFunc := func() (any, any, bool, error) {
		result, err := a.underlyingResult.RowsAffected()
		return result, nil, false, err
	}
	result, _, _, err := ExecuteWithPlugins(a.pluginManager, "Result.RowsAffected", execFunc)
	if err == nil {
		num, ok := result.(int64)
		if ok {
			return num, nil
		}
		err = errors.New(driver_infrastructure.GetMessage("AwsWrapperExecuteWithPlugins.unableToCastResult", "int64"))
	}
	return -1, err
}

type AwsWrapperTx struct {
	underlyingTx  driver.Tx
	pluginManager driver_infrastructure.PluginManager
}

func (a *AwsWrapperTx) Commit() error {
	commitFunc := func() (any, any, bool, error) { return nil, nil, false, a.underlyingTx.Commit() }
	_, _, _, err := ExecuteWithPlugins(a.pluginManager, "Tx.Commit", commitFunc)
	return err
}

func (a *AwsWrapperTx) Rollback() error {
	rollbackFunc := func() (any, any, bool, error) { return nil, nil, false, a.underlyingTx.Rollback() }
	_, _, _, err := ExecuteWithPlugins(a.pluginManager, "Tx.Rollback", rollbackFunc)
	return err
}

type AwsWrapperRows struct {
	underlyingRows driver.Rows
	pluginManager  driver_infrastructure.PluginManager
}

func (a *AwsWrapperRows) Close() error {
	closeFunc := func() (any, any, bool, error) { return nil, nil, false, a.underlyingRows.Close() }
	_, _, _, err := ExecuteWithPlugins(a.pluginManager, "Rows.Close", closeFunc)
	return err
}

func (a *AwsWrapperRows) Columns() []string {
	columnsFunc := func() (any, any, bool, error) { return a.underlyingRows.Columns(), nil, false, nil }
	result, _, _, _ := ExecuteWithPlugins(a.pluginManager, "Rows.Columns", columnsFunc)
	cols, ok := result.([]string)
	if ok {
		return cols
	}
	return nil
}

func (a *AwsWrapperRows) Next(dest []driver.Value) error {
	nextFunc := func() (any, any, bool, error) { return nil, nil, false, a.underlyingRows.Next(dest) }
	_, _, _, err := ExecuteWithPlugins(a.pluginManager, "Rows.Next", nextFunc)
	return err
}

func (a *AwsWrapperRows) ColumnTypePrecisionScale(index int) (int64, int64, bool) {
	rowInterface, ok := a.underlyingRows.(driver.RowsColumnTypePrecisionScale)
	if ok {
		rowFunc := func() (any, any, bool, error) {
			result1, result2, boolean := rowInterface.ColumnTypePrecisionScale(index)
			return result1, result2, boolean, nil
		}
		p, s, ok, _ := ExecuteWithPlugins(a.pluginManager, "Rows.ColumnTypePrecisionScale", rowFunc)
		precision, pOk := p.(int64)
		scale, sOk := s.(int64)
		if sOk && pOk {
			return precision, scale, ok
		}
	}
	return -1, -1, false
}

func (a *AwsWrapperRows) ColumnTypeDatabaseTypeName(index int) string {
	rowInterface, ok := a.underlyingRows.(driver.RowsColumnTypeDatabaseTypeName)
	if ok {
		rowFunc := func() (any, any, bool, error) { return rowInterface.ColumnTypeDatabaseTypeName(index), nil, false, nil }
		result, _, _, _ := ExecuteWithPlugins(a.pluginManager, "Rows.ColumnTypeDatabaseTypeName", rowFunc)
		str, ok := result.(string)
		if ok {
			return str
		}
	}
	return ""
}

type AwsWrapperPgRows struct {
	AwsWrapperRows
}

func (a *AwsWrapperPgRows) ColumnTypeLength(index int) (int64, bool) {
	rowInterface, ok := a.underlyingRows.(driver.RowsColumnTypeLength)
	if ok {
		rowFunc := func() (any, any, bool, error) {
			result, boolean := rowInterface.ColumnTypeLength(index)
			return result, nil, boolean, nil
		}
		result, _, ok, _ := ExecuteWithPlugins(a.pluginManager, "Rows.ColumnTypeLength", rowFunc)
		num, numOk := result.(int64)
		if numOk {
			return num, ok
		}
	}
	return -1, false
}

type AwsWrapperMySQLRows struct {
	AwsWrapperRows
}

func (a *AwsWrapperMySQLRows) HasNextResultSet() bool {
	rowInterface, ok := a.underlyingRows.(driver.RowsNextResultSet)
	if !ok {
		return false
	}
	rowFunc := func() (any, any, bool, error) { return nil, nil, rowInterface.HasNextResultSet(), nil }
	_, _, ok, _ = ExecuteWithPlugins(a.pluginManager, "Rows.HasNextResultSet", rowFunc)
	return ok
}

func (a *AwsWrapperMySQLRows) NextResultSet() error {
	rowInterface, ok := a.underlyingRows.(driver.RowsNextResultSet)
	if !ok {
		return errors.New(driver_infrastructure.GetMessage("AwsWrapperRows.underlyingRowsDoNotImplementRequiredInterface", "driver.RowsNextResultSet"))
	}
	rowFunc := func() (any, any, bool, error) { return nil, nil, false, rowInterface.NextResultSet() }
	_, _, _, err := ExecuteWithPlugins(a.pluginManager, "Rows.NextResultSet", rowFunc)
	return err
}

func (a *AwsWrapperMySQLRows) ColumnTypeScanType(index int) reflect.Type {
	rowInterface, ok := a.underlyingRows.(driver.RowsColumnTypeScanType)
	if ok {
		rowFunc := func() (any, any, bool, error) { return rowInterface.ColumnTypeScanType(index), nil, false, nil }
		result, _, _, _ := ExecuteWithPlugins(a.pluginManager, "Rows.ColumnTypeScanType", rowFunc)
		reflectType, ok := result.(reflect.Type)
		if ok {
			return reflectType
		}
	}
	return nil
}

func (a *AwsWrapperMySQLRows) ColumnTypeNullable(index int) (bool, bool) {
	rowInterface, ok := a.underlyingRows.(driver.RowsColumnTypeNullable)
	if ok {
		rowFunc := func() (any, any, bool, error) {
			boolean1, boolean2 := rowInterface.ColumnTypeNullable(index)
			return nil, boolean1, boolean2, nil
		}
		_, result, ok, _ := ExecuteWithPlugins(a.pluginManager, "Rows.ColumnTypeNullable", rowFunc)
		nullable, boolOk := result.(bool)
		if boolOk {
			return nullable, ok
		}
	}
	return false, false
}
