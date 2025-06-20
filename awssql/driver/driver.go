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
	"database/sql/driver"
	"errors"
	"log/slog"
	"reflect"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/efm"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/limitless"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
)

var pluginFactoryByCode = map[string]driver_infrastructure.ConnectionPluginFactory{
	"failover":      plugins.NewFailoverPluginFactory(),
	"efm":           efm.NewHostMonitoringPluginFactory(),
	"limitless":     limitless.NewLimitlessPluginFactory(),
	"executionTime": plugins.NewExecutionTimePluginFactory(),
}

type AwsWrapperDriver struct {
	DriverDialect    driver_infrastructure.DriverDialect
	UnderlyingDriver driver.Driver
}

func (d *AwsWrapperDriver) Open(dsn string) (driver.Conn, error) {
	if d.UnderlyingDriver == nil || d.DriverDialect == nil {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("Driver.missingUnderlyingDriverOrDialect"))
	}

	props, parseErr := utils.ParseDsn(dsn)
	if parseErr != nil {
		return nil, parseErr
	}
	slog.Debug((error_util.GetMessage("AwsWrapper.initializingDatabaseHandle", utils.MaskProperties(props))))

	defaultConnProvider := driver_infrastructure.NewDriverConnectionProvider(d.UnderlyingDriver)
	connectionProviderManager := driver_infrastructure.ConnectionProviderManager{DefaultProvider: defaultConnProvider}

	telemetryFactory, err := telemetry.NewDefaultTelemetryFactory(props)
	if err != nil {
		return nil, err
	}
	pluginManager := driver_infrastructure.PluginManager(plugin_helpers.NewPluginManagerImpl(d.UnderlyingDriver, props, connectionProviderManager, telemetryFactory))
	pluginServiceImpl, err := plugin_helpers.NewPluginServiceImpl(pluginManager, d.DriverDialect, props, dsn)
	if err != nil {
		return nil, err
	}
	pluginService := driver_infrastructure.PluginService(pluginServiceImpl)

	pluginChainBuilder := ConnectionPluginChainBuilder{}
	currentPlugins, err := pluginChainBuilder.GetPlugins(pluginService, pluginManager, props, pluginFactoryByCode)
	if err != nil {
		return nil, err
	}

	err = pluginManager.Init(pluginService, currentPlugins)
	if err != nil {
		return nil, err
	}

	hostListProviderService := driver_infrastructure.HostListProviderService(pluginServiceImpl)
	provider := hostListProviderService.CreateHostListProvider(props, dsn)
	hostListProviderService.SetHostListProvider(provider)

	telemetryCtx, ctx := pluginManager.GetTelemetryFactory().OpenTelemetryContext(telemetry.TELEMETRY_OPEN_CONNECTION, telemetry.TOP_LEVEL, nil)
	pluginManager.SetTelemetryContext(ctx)
	defer func() {
		telemetryCtx.CloseContext()
		pluginManager.SetTelemetryContext(context.TODO())
	}()

	err = pluginManager.InitHostProvider(dsn, props, hostListProviderService)
	if err != nil {
		return nil, err
	}

	refreshErr := pluginService.RefreshHostList(nil)
	if refreshErr != nil {
		return nil, refreshErr
	}

	dbEngine, dbEngineErr := GetDatabaseEngine(props)
	if dbEngineErr != nil {
		return nil, dbEngineErr
	}

	conn := pluginService.GetCurrentConnection()
	var connErr error
	if conn == nil {
		conn, connErr = pluginManager.Connect(pluginService.GetInitialConnectionHostInfo(), pluginService.GetProperties(), true)
		if connErr != nil {
			return nil, connErr
		}

		if conn == nil {
			return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("Driver.connectionNotOpen"))
		}

		err = pluginService.SetCurrentConnection(conn, pluginService.GetInitialConnectionHostInfo(), nil)
		if err != nil {
			return nil, err
		}

		refreshErr = pluginService.RefreshHostList(conn)
		if refreshErr != nil {
			return nil, refreshErr
		}
	}

	return NewAwsWrapperConn(pluginManager, pluginService, dbEngine), nil
}

func UsePluginFactory(code string, pluginFactory driver_infrastructure.ConnectionPluginFactory) {
	pluginFactoryByCode[code] = pluginFactory
}

func RemovePluginFactory(code string) {
	delete(pluginFactoryByCode, code)
}

// This cleans up all long standing caches. To be called at the end of program, not each time a Conn is closed.
func ClearCaches() {
	driver_infrastructure.ClearCaches()
	plugin_helpers.ClearCaches()
	for _, pluginFactory := range pluginFactoryByCode {
		pluginFactory.ClearCaches()
	}
	driver_infrastructure.MonitoringRdsHostListProviderClearCaches()
}

type AwsWrapperConn struct {
	pluginManager driver_infrastructure.PluginManager
	pluginService driver_infrastructure.PluginService
	engine        driver_infrastructure.DatabaseEngine
}

func NewAwsWrapperConn(
	pluginManager driver_infrastructure.PluginManager,
	pluginService driver_infrastructure.PluginService,
	engine driver_infrastructure.DatabaseEngine) *AwsWrapperConn {
	return &AwsWrapperConn{pluginManager, pluginService, engine}
}

func (c *AwsWrapperConn) Prepare(query string) (driver.Stmt, error) {
	prepareFunc := func() (any, any, bool, error) {
		result, err := c.pluginService.GetCurrentConnection().Prepare(query)
		return result, nil, false, err
	}
	return prepareWithPlugins(c.pluginService.GetCurrentConnection(), c.pluginManager, utils.CONN_PREPARE, prepareFunc, *c, query)
}

func (c *AwsWrapperConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	prepareFunc := func() (any, any, bool, error) {
		prepareCtx, ok := c.pluginService.GetCurrentConnection().(driver.ConnPrepareContext)
		if !ok {
			return nil, nil, false, errors.New(error_util.GetMessage("Conn.doesNotImplementRequiredInterface", "driver.ConnPrepareContext"))
		}
		result, err := prepareCtx.PrepareContext(ctx, query)
		return result, nil, false, err
	}
	return prepareWithPlugins(c.pluginService.GetCurrentConnection(), c.pluginManager, utils.CONN_PREPARE_CONTEXT, prepareFunc, *c, query)
}

func (c *AwsWrapperConn) Close() error {
	closeFunc := func() (any, any, bool, error) { return nil, nil, false, c.pluginService.GetCurrentConnection().Close() }
	_, _, _, err := ExecuteWithPlugins(c.pluginService.GetCurrentConnection(), c.pluginManager, utils.CONN_CLOSE, closeFunc)
	pluginManager, ok := c.pluginManager.(driver_infrastructure.CanReleaseResources)
	if ok {
		pluginManager.ReleaseResources()
	}
	return err
}

func (c *AwsWrapperConn) Begin() (driver.Tx, error) {
	beginFunc := func() (any, any, bool, error) {
		result, err := c.pluginService.GetCurrentConnection().Begin() //nolint:all
		return result, nil, false, err
	}
	return beginWithPlugins(c.pluginService.GetCurrentConnection(), c.pluginManager, c.pluginService, utils.CONN_BEGIN, beginFunc)
}

func (c *AwsWrapperConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	beginFunc := func() (any, any, bool, error) {
		beginTx, ok := c.pluginService.GetCurrentConnection().(driver.ConnBeginTx)
		if !ok {
			return nil, nil, false, errors.New(error_util.GetMessage("Conn.doesNotImplementRequiredInterface", "driver.ConnBeginTx"))
		}
		result, err := beginTx.BeginTx(ctx, opts)
		return result, nil, false, err
	}
	return beginWithPlugins(c.pluginService.GetCurrentConnection(), c.pluginManager, c.pluginService, utils.CONN_BEGIN_TX, beginFunc)
}

func (c *AwsWrapperConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	queryFunc := func() (any, any, bool, error) {
		queryerCtx, ok := c.pluginService.GetCurrentConnection().(driver.QueryerContext)
		if !ok {
			return nil, nil, false, errors.New(error_util.GetMessage("Conn.doesNotImplementRequiredInterface", "driver.QueryerContext"))
		}
		result, err := queryerCtx.QueryContext(ctx, query, args)
		return result, nil, false, err
	}
	return queryWithPlugins(c.pluginService.GetCurrentConnection(), c.pluginManager, utils.CONN_QUERY_CONTEXT, queryFunc, c.engine, query)
}

func (c *AwsWrapperConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	execFunc := func() (any, any, bool, error) {
		execerCtx, ok := c.pluginService.GetCurrentConnection().(driver.ExecerContext)
		if !ok {
			return nil, nil, false, errors.New(error_util.GetMessage("Conn.doesNotImplementRequiredInterface", "driver.ExecerContext"))
		}
		result, err := execerCtx.ExecContext(ctx, query, args)
		return result, nil, false, err
	}
	return execWithPlugins(c.pluginService.GetCurrentConnection(), c.pluginManager, utils.CONN_EXEC_CONTEXT, execFunc, query)
}

func (c *AwsWrapperConn) Ping(ctx context.Context) error {
	pingFunc := func() (any, any, bool, error) {
		pinger, ok := c.pluginService.GetCurrentConnection().(driver.Pinger)
		if !ok {
			return nil, nil, false, errors.New(error_util.GetMessage("Conn.doesNotImplementRequiredInterface", "driver.Pinger"))
		}
		return nil, nil, false, pinger.Ping(ctx)
	}
	_, _, _, err := ExecuteWithPlugins(c.pluginService.GetCurrentConnection(), c.pluginManager, utils.CONN_PING, pingFunc)
	return err
}

func (c *AwsWrapperConn) IsValid() bool {
	isValidFunc := func() (any, any, bool, error) {
		validator, ok := c.pluginService.GetCurrentConnection().(driver.Validator)
		if ok {
			return nil, nil, validator.IsValid(), nil
		}
		return nil, nil, true, nil
	}
	_, _, result, _ := ExecuteWithPlugins(c.pluginService.GetCurrentConnection(), c.pluginManager, utils.CONN_IS_VALID, isValidFunc)
	return result
}

func (c *AwsWrapperConn) ResetSession(ctx context.Context) error {
	resetSessionFunc := func() (any, any, bool, error) {
		resetter, ok := c.pluginService.GetCurrentConnection().(driver.SessionResetter)
		if !ok {
			return nil, nil, false, errors.New(error_util.GetMessage("Conn.doesNotImplementRequiredInterface", "driver.SessionResetter"))
		}
		return nil, nil, false, resetter.ResetSession(ctx)
	}
	_, _, _, err := ExecuteWithPlugins(c.pluginService.GetCurrentConnection(), c.pluginManager, utils.CONN_RESET_SESSION, resetSessionFunc)
	return err
}

func (c *AwsWrapperConn) CheckNamedValue(val *driver.NamedValue) error {
	checkNamedValueFunc := func() (any, any, bool, error) {
		namedValueChecker, ok := c.pluginService.GetCurrentConnection().(driver.NamedValueChecker)
		if !ok {
			return nil, nil, false, errors.New(error_util.GetMessage("Conn.doesNotImplementRequiredInterface", "driver.NamedValueChecker"))
		}
		return nil, nil, false, namedValueChecker.CheckNamedValue(val)
	}
	_, _, _, err := ExecuteWithPlugins(c.pluginService.GetCurrentConnection(), c.pluginManager, utils.CONN_CHECK_NAMED_VALUE, checkNamedValueFunc)
	return err
}

type AwsWrapperStmt struct {
	underlyingConn driver.Conn
	underlyingStmt driver.Stmt
	pluginManager  driver_infrastructure.PluginManager
	conn           AwsWrapperConn
}

func (a *AwsWrapperStmt) Close() error {
	closeFunc := func() (any, any, bool, error) { return nil, nil, false, a.underlyingStmt.Close() }
	_, _, _, err := ExecuteWithPlugins(a.underlyingConn, a.pluginManager, utils.STMT_CLOSE, closeFunc)
	return err
}

func (a *AwsWrapperStmt) Exec(args []driver.Value) (driver.Result, error) {
	execFunc := func() (any, any, bool, error) {
		result, err := a.underlyingStmt.Exec(args) //nolint:all
		return result, nil, false, err
	}
	return execWithPlugins(a.underlyingConn, a.pluginManager, utils.STMT_EXEC, execFunc)
}

func (a *AwsWrapperStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	execerCtx, ok := a.underlyingStmt.(driver.StmtExecContext)
	if !ok {
		return nil, errors.New(error_util.GetMessage("AwsWrapperStmt.underlyingStmtDoesNotImplementRequiredInterface", "driver.StmtExecContext"))
	}
	execFunc := func() (any, any, bool, error) {
		result, err := execerCtx.ExecContext(ctx, args)
		return result, nil, false, err
	}
	return execWithPlugins(a.underlyingConn, a.pluginManager, utils.STMT_EXEC_CONTEXT, execFunc)
}

func (a *AwsWrapperStmt) NumInput() int {
	numInputFunc := func() (any, any, bool, error) { return a.underlyingStmt.NumInput(), nil, false, nil }
	result, _, _, _ := ExecuteWithPlugins(a.underlyingConn, a.pluginManager, utils.STMT_NUM_INPUT, numInputFunc)
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
	return queryWithPlugins(a.underlyingConn, a.pluginManager, utils.STMT_QUERY, queryFunc, a.conn.engine)
}

func (a *AwsWrapperStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	queryerCtx, ok := a.underlyingStmt.(driver.StmtQueryContext)
	if !ok {
		return nil, errors.New(error_util.GetMessage("AwsWrapperStmt.underlyingStmtDoesNotImplementRequiredInterface", "driver.StmtQueryContext"))
	}
	queryFunc := func() (any, any, bool, error) {
		result, err := queryerCtx.QueryContext(ctx, args)
		return result, nil, false, err
	}
	return queryWithPlugins(a.underlyingConn, a.pluginManager, utils.STMT_QUERY_CONTEXT, queryFunc, a.conn.engine)
}

func (a *AwsWrapperStmt) CheckNamedValue(val *driver.NamedValue) error {
	namedValueChecker, ok := a.underlyingStmt.(driver.NamedValueChecker)
	if !ok {
		// If underlyingStmt does not implement the NamedValueChecker interface, fallback to the conn the statement was prepared from.
		return a.conn.CheckNamedValue(val)
	}
	checkNamedValueFunc := func() (any, any, bool, error) { return nil, nil, false, namedValueChecker.CheckNamedValue(val) }
	_, _, _, err := ExecuteWithPlugins(a.underlyingConn, a.pluginManager, utils.STMT_CHECK_NAMED_VALUE, checkNamedValueFunc)
	return err
}

type AwsWrapperResult struct {
	underlyingConn   driver.Conn
	underlyingResult driver.Result
	pluginManager    driver_infrastructure.PluginManager
}

func (a *AwsWrapperResult) LastInsertId() (int64, error) {
	execFunc := func() (any, any, bool, error) {
		result, err := a.underlyingResult.LastInsertId()
		return result, nil, false, err
	}
	result, _, _, err := ExecuteWithPlugins(a.underlyingConn, a.pluginManager, utils.RESULT_LAST_INSERT_ID, execFunc)
	if err == nil {
		num, ok := result.(int64)
		if ok {
			return num, nil
		}
		err = errors.New(error_util.GetMessage("AwsWrapperExecuteWithPlugins.unableToCastResult", "int64"))
	}
	return -1, err
}

func (a *AwsWrapperResult) RowsAffected() (int64, error) {
	execFunc := func() (any, any, bool, error) {
		result, err := a.underlyingResult.RowsAffected()
		return result, nil, false, err
	}
	result, _, _, err := ExecuteWithPlugins(a.underlyingConn, a.pluginManager, utils.RESULT_ROWS_AFFECTED, execFunc)
	if err == nil {
		num, ok := result.(int64)
		if ok {
			return num, nil
		}
		err = errors.New(error_util.GetMessage("AwsWrapperExecuteWithPlugins.unableToCastResult", "int64"))
	}
	return -1, err
}

type AwsWrapperTx struct {
	underlyingConn driver.Conn
	underlyingTx   driver.Tx
	pluginManager  driver_infrastructure.PluginManager
	pluginService  driver_infrastructure.PluginService
}

func (a *AwsWrapperTx) Commit() error {
	commitFunc := func() (any, any, bool, error) { return nil, nil, false, a.underlyingTx.Commit() }
	_, _, _, err := ExecuteWithPlugins(a.underlyingConn, a.pluginManager, utils.TX_COMMIT, commitFunc)
	a.pluginService.SetCurrentTx(nil)
	return err
}

func (a *AwsWrapperTx) Rollback() error {
	rollbackFunc := func() (any, any, bool, error) { return nil, nil, false, a.underlyingTx.Rollback() }
	_, _, _, err := ExecuteWithPlugins(a.underlyingConn, a.pluginManager, utils.TX_ROLLBACK, rollbackFunc)
	a.pluginService.SetCurrentTx(nil)
	return err
}

type AwsWrapperRows struct {
	underlyingConn driver.Conn
	underlyingRows driver.Rows
	pluginManager  driver_infrastructure.PluginManager
}

func (a *AwsWrapperRows) Close() error {
	closeFunc := func() (any, any, bool, error) { return nil, nil, false, a.underlyingRows.Close() }
	_, _, _, err := ExecuteWithPlugins(a.underlyingConn, a.pluginManager, utils.ROWS_CLOSE, closeFunc)
	return err
}

func (a *AwsWrapperRows) Columns() []string {
	columnsFunc := func() (any, any, bool, error) { return a.underlyingRows.Columns(), nil, false, nil }
	result, _, _, _ := ExecuteWithPlugins(a.underlyingConn, a.pluginManager, utils.ROWS_COLUMNS, columnsFunc)
	cols, ok := result.([]string)
	if ok {
		return cols
	}
	return nil
}

func (a *AwsWrapperRows) Next(dest []driver.Value) error {
	nextFunc := func() (any, any, bool, error) { return nil, nil, false, a.underlyingRows.Next(dest) }
	_, _, _, err := ExecuteWithPlugins(a.underlyingConn, a.pluginManager, utils.ROWS_NEXT, nextFunc)
	return err
}

func (a *AwsWrapperRows) ColumnTypePrecisionScale(index int) (int64, int64, bool) {
	rowInterface, ok := a.underlyingRows.(driver.RowsColumnTypePrecisionScale)
	if ok {
		rowFunc := func() (any, any, bool, error) {
			result1, result2, boolean := rowInterface.ColumnTypePrecisionScale(index)
			return result1, result2, boolean, nil
		}
		p, s, ok, _ := ExecuteWithPlugins(a.underlyingConn, a.pluginManager, utils.ROWS_COLUMN_TYPE_PRECISION_SCALE, rowFunc)
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
		result, _, _, _ := ExecuteWithPlugins(a.underlyingConn, a.pluginManager, utils.ROWS_COLUMN_TYPE_DATABASE_TYPE_NAME, rowFunc)
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
		result, _, ok, _ := ExecuteWithPlugins(a.underlyingConn, a.pluginManager, utils.ROWS_COLUMN_TYPE_LENGTH, rowFunc)
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
	_, _, ok, _ = ExecuteWithPlugins(a.underlyingConn, a.pluginManager, utils.ROWS_HAS_NEXT_RESULT_SET, rowFunc)
	return ok
}

func (a *AwsWrapperMySQLRows) NextResultSet() error {
	rowInterface, ok := a.underlyingRows.(driver.RowsNextResultSet)
	if !ok {
		return errors.New(error_util.GetMessage("AwsWrapperRows.underlyingRowsDoNotImplementRequiredInterface", "driver.RowsNextResultSet"))
	}
	rowFunc := func() (any, any, bool, error) { return nil, nil, false, rowInterface.NextResultSet() }
	_, _, _, err := ExecuteWithPlugins(a.underlyingConn, a.pluginManager, utils.ROWS_NEXT_RESULT_SET, rowFunc)
	return err
}

func (a *AwsWrapperMySQLRows) ColumnTypeScanType(index int) reflect.Type {
	rowInterface, ok := a.underlyingRows.(driver.RowsColumnTypeScanType)
	if ok {
		rowFunc := func() (any, any, bool, error) { return rowInterface.ColumnTypeScanType(index), nil, false, nil }
		result, _, _, _ := ExecuteWithPlugins(a.underlyingConn, a.pluginManager, utils.ROWS_COLUMN_TYPE_SCAN_TYPE, rowFunc)
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
		_, result, ok, _ := ExecuteWithPlugins(a.underlyingConn, a.pluginManager, utils.ROWS_COLUMN_TYPE_NULLABLE, rowFunc)
		nullable, boolOk := result.(bool)
		if boolOk {
			return nullable, ok
		}
	}
	return false, false
}
