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

package test

import (
	"awssql/container"
	awsDriver "awssql/driver"
	"awssql/driver_infrastructure"
	"awssql/error_util"
	"awssql/host_info_util"
	"awssql/plugin_helpers"
	"context"
	"database/sql/driver"
	"strings"
	"testing"
)

var mysqlTestDsn = "someUser:somePassword@tcp(mydatabase.com:3306)/myDatabase?foo=bar&pop=snap"
var pgTestDsn = "postgres://someUser:somePassword@localhost:5432/pgx_test?sslmode=disable&foo=bar"

func TestAwsWrapperError(t *testing.T) {
	testError := error_util.NewUnavailableHostError("test")
	if testError.IsFailoverErrorType() {
		t.Errorf("Should return false, UnavailableHostError is not a failover error type.")
	}
	if !testError.IsType(error_util.UnavailableHostErrorType) {
		t.Errorf("Should return true, error is a UnavailableHostErrorType.")
	}
	if testError.IsType(error_util.UnsupportedMethodErrorType) {
		t.Errorf("Should return false, error is not a UnsupportedMethodErrorType.")
	}
	if !strings.Contains(testError.Error(), "test") {
		t.Errorf("Error should include 'test', improperly handles message.")
	}

	if !error_util.FailoverSuccessError.IsFailoverErrorType() {
		t.Errorf("Should return true, FailoverSuccessError is a failover error type.")
	}
	if !error_util.FailoverSuccessError.IsType(error_util.FailoverSuccessErrorType) {
		t.Errorf("Should return true, error is a FailoverSuccessErrorType.")
	}
	if error_util.FailoverSuccessError.IsType(error_util.UnsupportedMethodErrorType) {
		t.Errorf("Should return false, error is not a UnsupportedMethodErrorType.")
	}
	if error_util.FailoverSuccessError.Error() != error_util.GetMessage("Failover.connectionChangedError") {
		t.Errorf("Should return message attached to Failover.connectionChangedError, improperly handles message.")
	}
}

func TestIsDialectPg(t *testing.T) {
	testConn := MockDriverConn{nil}
	pgDialect := driver_infrastructure.PgDatabaseDialect{}
	implementsQueryer := pgDialect.IsDialect(testConn)
	if implementsQueryer {
		t.Errorf("Should return false, connection does not implement QueryContext.")
	}
	testConnWithQuery := &MockConn{nil, nil, nil, nil, true}
	passesQuery := pgDialect.IsDialect(testConnWithQuery)
	if passesQuery {
		t.Errorf("Should return false, QueryContext throws an error.")
	}
	testConnWithQuery.updateQueryRow(make([]string, 0), make([]driver.Value, 0))
	testConnWithQuery.updateThrowError(false)
	returnsRow := pgDialect.IsDialect(testConnWithQuery)
	if returnsRow {
		t.Errorf("Should return false, query does not result in a row.")
	}
	testConnWithQuery.updateQueryRow([]string{"column"}, []driver.Value{"test"})
	returnsRow = pgDialect.IsDialect(testConnWithQuery)
	if !returnsRow {
		t.Errorf("Should return true, query results in a row.")
	}

	rdsPgDialect := driver_infrastructure.RdsPgDatabaseDialect{}

	testConnWithQuery.updateQueryRow([]string{"rds", "aurora"}, []driver.Value{false, false})
	returnsRow = rdsPgDialect.IsDialect(testConnWithQuery)
	if returnsRow {
		t.Errorf("Should return false as is a PG dialect.")
	}
	testConnWithQuery.updateQueryRow([]string{"rds", "aurora"}, []driver.Value{true, true})
	returnsRow = rdsPgDialect.IsDialect(testConnWithQuery)
	if returnsRow {
		t.Errorf("Should return false as is an Aurora dialect.")
	}
	testConnWithQuery.updateQueryRow([]string{"rds", "aurora"}, []driver.Value{true, false})
	returnsRow = rdsPgDialect.IsDialect(testConnWithQuery)
	if !returnsRow {
		t.Errorf("Should return true as is a RDS dialect.")
	}

	auroraPgDialect := driver_infrastructure.AuroraPgDatabaseDialect{}

	testConnWithQuery.updateQueryRow([]string{"extensions"}, []driver.Value{false})
	returnsRow = auroraPgDialect.IsDialect(testConnWithQuery)
	if returnsRow {
		t.Errorf("Should return false as is not an Aurora dialect.")
	}
	testConnWithQuery.updateQueryRow([]string{"extensions"}, []driver.Value{true})
	returnsRow = auroraPgDialect.IsDialect(testConnWithQuery)
	if !returnsRow {
		t.Errorf("Should return true as is an Aurora dialect.")
	}
}

func TestIsDialectMySQL(t *testing.T) {
	mySqlDialect := driver_infrastructure.MySQLDatabaseDialect{}
	testConnWithQuery := &MockConn{nil, nil, nil, nil, true}

	testConnWithQuery.updateQueryRow([]string{"variable_name"}, []driver.Value{"version_comment"})
	returnsRow := mySqlDialect.IsDialect(testConnWithQuery)
	if returnsRow {
		t.Errorf("Should return false as needed value is out of range.")
	}

	testConnWithQuery.updateQueryRow(
		[]string{"variable_name", "value"},
		[]driver.Value{"version_comment", []uint8{109, 121, 115, 113, 108}})
	returnsRow = mySqlDialect.IsDialect(testConnWithQuery)
	if returnsRow {
		t.Errorf("Should return false as query result does not contains 'MySQL'.")
	}

	testConnWithQuery.updateQueryRow(
		[]string{"variable_name", "value"},
		[]driver.Value{"version_comment", []uint8{77, 121, 83, 81, 76}})
	returnsRow = mySqlDialect.IsDialect(testConnWithQuery)
	if !returnsRow {
		t.Errorf("Should return true as query result contains 'MySQL'.")
	}

	rdsMySQLDialect := driver_infrastructure.RdsMySQLDatabaseDialect{}

	testConnWithQuery.updateQueryRow([]string{"variable_name"}, []driver.Value{"version_comment"})
	returnsRow = rdsMySQLDialect.IsDialect(testConnWithQuery)
	if returnsRow {
		t.Errorf("Should return false as needed value is out of range.")
	}

	testConnWithQuery.updateQueryRow([]string{"variable_name", "value"}, []driver.Value{"version_comment", []uint8{1, 2, 3}})
	returnsRow = rdsMySQLDialect.IsDialect(testConnWithQuery)
	if returnsRow {
		t.Errorf("Should return false as query result does not contains 'Source Distribution'.")
	}

	testConnWithQuery.updateQueryRow(
		[]string{"variable_name", "value"},
		[]driver.Value{
			"version_comment",
			[]uint8{83, 111, 117, 114, 99, 101, 32, 100, 105, 115, 116, 114, 105, 98, 117, 116, 105, 111, 110}})
	returnsRow = rdsMySQLDialect.IsDialect(testConnWithQuery)
	if !returnsRow {
		t.Errorf("Should return true as query result contains 'Source Distribution'.")
	}

	auroraMySQLDialect := driver_infrastructure.AuroraMySQLDatabaseDialect{}

	testConnWithQuery.updateQueryRow([]string{}, []driver.Value{})
	returnsRow = auroraMySQLDialect.IsDialect(testConnWithQuery)
	if returnsRow {
		t.Errorf("Should return false as needed value is out of range.")
	}

	testConnWithQuery.updateQueryRow([]string{"column"}, []driver.Value{"test"})
	returnsRow = auroraMySQLDialect.IsDialect(testConnWithQuery)
	if !returnsRow {
		t.Errorf("Should return true as query result returns a row.")
	}
}

func TestWrapperUtilsQueryWithPluginsMySQL(t *testing.T) {
	props := map[string]string{"protocol": "mysql"}
	mockPluginManager := driver_infrastructure.PluginManager(plugin_helpers.NewPluginManagerImpl(nil, props))
	pluginServiceImpl, _ := plugin_helpers.NewPluginServiceImpl(mockPluginManager, driver_infrastructure.MySQLDriverDialect{}, props, mysqlTestDsn)
	mockPluginService := driver_infrastructure.PluginService(pluginServiceImpl)
	plugins := []driver_infrastructure.ConnectionPlugin{
		CreateTestPlugin(nil, 1, nil, nil, false),
	}
	_ = mockPluginManager.Init(nil, plugins, driver_infrastructure.ConnectionProviderManager{})
	mockContainer := container.Container{PluginManager: mockPluginManager, PluginService: mockPluginService}
	baseAwsWrapperConn := *awsDriver.NewAwsWrapperConn(mockContainer, driver_infrastructure.MYSQL)
	res, err := baseAwsWrapperConn.QueryContext(context.Background(), "", nil)
	if res != nil ||
		!strings.Contains(
			err.Error(),
			"The underlying driver connection does not implement the required interface 'driver.QueryerContext'.") {
		t.Errorf("An AWS Wrapper Conn with an underlying connection that does not support QueryContext should not return a result.")
	}

	mockUnderlyingConn := &MockConn{nil, nil, nil, nil, false}
	mockUnderlyingConn.updateQueryRow([]string{"column"}, []driver.Value{"test"})
	mockAwsWrapperConn := *awsDriver.NewAwsWrapperConn(mockContainer, driver_infrastructure.MYSQL)
	_ = mockPluginService.SetCurrentConnection(mockUnderlyingConn, host_info_util.HostInfo{}, nil)
	res, err = mockAwsWrapperConn.QueryContext(context.Background(), "", nil)
	if err != nil || res.Columns()[0] != "column" {
		t.Errorf("An AWS Wrapper Conn with an underlying connection that does support QueryContext should return a result.")
	}

	mysqlRows, ok := res.(*awsDriver.AwsWrapperMySQLRows)
	if ok == false {
		t.Errorf("Wrapped QueryContext with DatabaseEngine MYSQL should return type of AwsWrapperMySQLRows.")
	}

	err = mysqlRows.NextResultSet()
	if !strings.Contains(err.Error(), "The underlying rows do not implement the required interface 'driver.RowsNextResultSet'.") {
		t.Errorf("The returned rows should attempt MySQL supported optional interfaces.")
	}

	result, ok := res.(driver.RowsColumnTypeLength)
	if ok || result != nil {
		t.Errorf("An AWS Wrapper Conn with an underlying MySQL connection should not implement driver.RowsColumnTypeLength.")
	}
}

func TestWrapperUtilsQueryWithPluginsPg(t *testing.T) {
	props := map[string]string{"protocol": "postgres"}
	mockPluginManager := driver_infrastructure.PluginManager(plugin_helpers.NewPluginManagerImpl(nil, props))
	pluginServiceImpl, _ := plugin_helpers.NewPluginServiceImpl(mockPluginManager, nil, props, pgTestDsn)
	mockPluginService := driver_infrastructure.PluginService(pluginServiceImpl)
	plugins := []driver_infrastructure.ConnectionPlugin{
		CreateTestPlugin(nil, 1, nil, nil, false),
	}
	_ = mockPluginManager.Init(nil, plugins, driver_infrastructure.ConnectionProviderManager{})
	mockContainer := container.Container{PluginManager: mockPluginManager, PluginService: mockPluginService}

	baseAwsWrapperConn := *awsDriver.NewAwsWrapperConn(mockContainer, driver_infrastructure.PG)
	res, err := baseAwsWrapperConn.QueryContext(context.Background(), "", nil)
	if res != nil ||
		!strings.Contains(
			err.Error(),
			"The underlying driver connection does not implement the required interface 'driver.QueryerContext'.") {
		t.Errorf("An AWS Wrapper Conn with an underlying connection that does not support QueryContext should not return a result.")
	}

	mockUnderlyingConn := &MockConn{nil, nil, nil, nil, false}
	_ = mockPluginService.SetCurrentConnection(mockUnderlyingConn, host_info_util.HostInfo{}, nil)

	mockUnderlyingConn.updateQueryRow([]string{"column"}, []driver.Value{"test"})
	mockAwsWrapperConn := *awsDriver.NewAwsWrapperConn(mockContainer, driver_infrastructure.PG)
	res, err = mockAwsWrapperConn.QueryContext(context.Background(), "", nil)
	if err != nil || res.Columns()[0] != "column" {
		t.Errorf("An AWS Wrapper Conn with an underlying connection that does support QueryContext should return a result.")
	}

	pgRows, ok := res.(*awsDriver.AwsWrapperPgRows)
	if ok == false {
		t.Errorf("Wrapped QueryContext with DatabaseEngine PG should return type of AwsWrapperPgRows.")
	}

	num, boolean := pgRows.ColumnTypeLength(0)
	if num != -1 && boolean {
		t.Errorf("The returned rows should attempt PG supported optional interfaces.")
	}

	result, ok := res.(driver.RowsColumnTypeScanType)
	if ok || result != nil {
		t.Errorf("An AWS Wrapper Conn with an underlying PG connection should not implement driver.RowsColumnTypeLength.")
	}
}

func TestWrapperUtilsExecWithPlugins(t *testing.T) {
	props := map[string]string{"protocol": "postgres"}
	mockPluginManager := driver_infrastructure.PluginManager(plugin_helpers.NewPluginManagerImpl(nil, props))
	pluginServiceImpl, _ := plugin_helpers.NewPluginServiceImpl(mockPluginManager, nil, props, pgTestDsn)
	mockPluginService := driver_infrastructure.PluginService(pluginServiceImpl)
	plugins := []driver_infrastructure.ConnectionPlugin{
		CreateTestPlugin(nil, 1, nil, nil, false),
	}
	_ = mockPluginManager.Init(nil, plugins, driver_infrastructure.ConnectionProviderManager{})
	mockContainer := container.Container{PluginManager: mockPluginManager, PluginService: mockPluginService}

	mockUnderlyingConn := &MockConn{nil, MockResult{}, nil, nil, false}
	_ = mockPluginService.SetCurrentConnection(mockUnderlyingConn, host_info_util.HostInfo{}, nil)
	mockAwsWrapperConn := *awsDriver.NewAwsWrapperConn(mockContainer, driver_infrastructure.PG)

	res, err := mockAwsWrapperConn.ExecContext(context.Background(), "", nil)
	if err != nil || res == nil {
		t.Errorf("An AWS Wrapper Conn with an underlying connection that does support ExecContext should return a result.")
	}

	awsResult, ok := res.(*awsDriver.AwsWrapperResult)
	if ok == false {
		t.Errorf("Wrapped ExecContext should return type of AwsWrapperResult.")
	}

	num, err := awsResult.RowsAffected()
	if num != -1 && !strings.Contains(err.Error(), "MockResult") {
		t.Errorf("Wrapped RowsAffected should return results of operations on underlyingResult.")
	}
}

func TestWrapperUtilsBeginWithPlugins(t *testing.T) {
	props := map[string]string{"protocol": "postgres"}
	mockPluginManager := driver_infrastructure.PluginManager(plugin_helpers.NewPluginManagerImpl(nil, props))
	pluginServiceImpl, _ := plugin_helpers.NewPluginServiceImpl(mockPluginManager, nil, props, pgTestDsn)
	mockPluginService := driver_infrastructure.PluginService(pluginServiceImpl)
	plugins := []driver_infrastructure.ConnectionPlugin{
		CreateTestPlugin(nil, 1, nil, nil, false),
	}
	_ = mockPluginManager.Init(nil, plugins, driver_infrastructure.ConnectionProviderManager{})
	mockContainer := container.Container{PluginManager: mockPluginManager, PluginService: mockPluginService}

	mockUnderlyingConn := &MockConn{nil, nil, MockTx{}, nil, false}
	_ = mockPluginService.SetCurrentConnection(mockUnderlyingConn, host_info_util.HostInfo{}, nil)
	mockAwsWrapperConn := *awsDriver.NewAwsWrapperConn(mockContainer, driver_infrastructure.PG)

	tx, err := mockAwsWrapperConn.Begin()
	if err != nil || tx == nil {
		t.Errorf("An AWS Wrapper Conn with an underlying connection should return a result to Begin.")
	}

	awsTx, ok := tx.(*awsDriver.AwsWrapperTx)
	if ok == false {
		t.Errorf("Wrapped Begin should return type of AwsWrapperTx.")
	}

	err = awsTx.Commit()
	if err == nil || !strings.Contains(err.Error(), "MockTx") {
		t.Errorf("Wrapped Commit should return results of operations on underlyingTx.")
	}
}

func TestWrapperUtilsPrepareWithPlugins(t *testing.T) {
	props := map[string]string{"protocol": "postgres"}
	mockPluginManager := driver_infrastructure.PluginManager(plugin_helpers.NewPluginManagerImpl(nil, props))
	pluginServiceImpl, _ := plugin_helpers.NewPluginServiceImpl(mockPluginManager, nil, props, pgTestDsn)
	mockPluginService := driver_infrastructure.PluginService(pluginServiceImpl)
	plugins := []driver_infrastructure.ConnectionPlugin{
		CreateTestPlugin(nil, 1, nil, nil, false),
	}
	_ = mockPluginManager.Init(nil, plugins, driver_infrastructure.ConnectionProviderManager{})
	mockContainer := container.Container{PluginManager: mockPluginManager, PluginService: mockPluginService}

	mockUnderlyingConn := &MockConn{nil, nil, nil, MockStmt{}, false}
	_ = mockPluginService.SetCurrentConnection(mockUnderlyingConn, host_info_util.HostInfo{}, nil)
	mockAwsWrapperConn := *awsDriver.NewAwsWrapperConn(mockContainer, driver_infrastructure.PG)

	res, err := mockAwsWrapperConn.Prepare("")
	if err != nil || res == nil {
		t.Errorf("An AWS Wrapper Conn with an underlying connection should return a result to Prepare.")
	}

	awsStmt, ok := res.(*awsDriver.AwsWrapperStmt)
	if ok == false {
		t.Errorf("Wrapped Prepare should return type of AwsWrapperStmt.")
	}

	val, err := awsStmt.Exec(nil)
	if val != nil || !strings.Contains(err.Error(), "MockStmt") {
		t.Errorf("Wrapped Exec should return results of operations on underlyingStmt.")
	}

	val, err = awsStmt.ExecContext(context.Background(), nil)
	if val != nil ||
		!strings.Contains(
			err.Error(),
			"The underlying driver statement does not implement the required interface 'driver.StmtExecContext'.") {
		t.Errorf("The returned stmt should attempt optional interfaces that are supported by both pgx and mysql drivers.")
	}
}
