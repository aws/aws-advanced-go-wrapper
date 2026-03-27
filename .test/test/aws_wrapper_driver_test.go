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
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	mock_telemetry "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/util/telemetry"
	mock_database_sql_driver "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/database_sql_driver"
	awsDriver "github.com/aws/aws-advanced-go-wrapper/awssql/driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	mysql_driver "github.com/aws/aws-advanced-go-wrapper/mysql-driver"
	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"
)

var mockHostInfo, _ = host_info_util.NewHostInfoBuilder().SetHost("test").SetPort(1234).SetRole(host_info_util.WRITER).Build()

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
	testConnWithQuery := &MockConn{throwError: true}
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
	testConnWithQuery := &MockConn{throwError: true}

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

// setupMockPluginManagerAndService creates gomock-based mocks for PluginManager and PluginService.
// The PluginManager.Execute mock delegates to the passed-in executeFunc (mimicking a passthrough plugin chain).
func setupMockPluginManagerAndService(
	ctrl *gomock.Controller,
	currentConn driver.Conn,
) (*mock_driver_infrastructure.MockPluginManager, *mock_driver_infrastructure.MockPluginService) {
	mockPluginManager := mock_driver_infrastructure.NewMockPluginManager(ctrl)
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockTelemetryFactory := mock_telemetry.NewMockTelemetryFactory(ctrl)
	mockTelemetryContext := mock_telemetry.NewMockTelemetryContext(ctrl)

	mockPluginService.EXPECT().GetCurrentConnection().Return(currentConn).AnyTimes()
	mockPluginService.EXPECT().UpdateState(gomock.Any(), gomock.Any()).AnyTimes()
	mockPluginManager.EXPECT().GetTelemetryFactory().Return(mockTelemetryFactory).AnyTimes()
	mockTelemetryFactory.EXPECT().OpenTelemetryContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(mockTelemetryContext, context.TODO()).AnyTimes()
	mockTelemetryContext.EXPECT().SetAttribute(gomock.Any(), gomock.Any()).AnyTimes()
	mockPluginManager.EXPECT().SetTelemetryContext(gomock.Any()).AnyTimes()
	mockTelemetryContext.EXPECT().CloseContext().AnyTimes()
	mockTelemetryContext.EXPECT().SetSuccess(gomock.Any()).AnyTimes()
	mockTelemetryContext.EXPECT().SetError(gomock.Any()).AnyTimes()

	mockPluginManager.EXPECT().
		Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(conn driver.Conn, methodName string, execFunc driver_infrastructure.ExecuteFunc, args ...any) (any, any, bool, error) {
			return execFunc()
		}).AnyTimes()

	return mockPluginManager, mockPluginService
}

func TestWrapperUtilsQueryWithPluginsMySQL(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginManager, mockPluginService := setupMockPluginManagerAndService(ctrl, nil)
	baseAwsWrapperConn := *awsDriver.NewAwsWrapperConn(mockPluginManager, mockPluginService, driver_infrastructure.MYSQL)
	res, err := baseAwsWrapperConn.QueryContext(context.Background(), "", nil)
	if res != nil ||
		!strings.Contains(
			err.Error(),
			"does not implement the required interface") {
		t.Errorf("An AWS Wrapper Conn with an underlying connection that does not support QueryContext should not return a result.")
	}

	mockUnderlyingConn := &MockConn{}
	mockUnderlyingConn.updateQueryRow([]string{"column"}, []driver.Value{"test"})
	mockPluginManager2, mockPluginService2 := setupMockPluginManagerAndService(ctrl, mockUnderlyingConn)
	mockAwsWrapperConn := awsDriver.NewAwsWrapperConn(mockPluginManager2, mockPluginService2, driver_infrastructure.MYSQL)
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginManager, mockPluginService := setupMockPluginManagerAndService(ctrl, nil)
	baseAwsWrapperConn := *awsDriver.NewAwsWrapperConn(mockPluginManager, mockPluginService, driver_infrastructure.PG)
	res, err := baseAwsWrapperConn.QueryContext(context.Background(), "", nil)
	if res != nil ||
		!strings.Contains(
			err.Error(),
			"does not implement the required interface") {
		t.Errorf("An AWS Wrapper Conn with an underlying connection that does not support QueryContext should not return a result.")
	}

	mockUnderlyingConn := &MockConn{}
	mockUnderlyingConn.updateQueryRow([]string{"column"}, []driver.Value{"test"})
	mockPluginManager2, mockPluginService2 := setupMockPluginManagerAndService(ctrl, mockUnderlyingConn)
	mockAwsWrapperConn := *awsDriver.NewAwsWrapperConn(mockPluginManager2, mockPluginService2, driver_infrastructure.PG)
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockUnderlyingConn := &MockConn{execResult: MockResult{}}
	mockPluginManager, mockPluginService := setupMockPluginManagerAndService(ctrl, mockUnderlyingConn)
	mockAwsWrapperConn := *awsDriver.NewAwsWrapperConn(mockPluginManager, mockPluginService, driver_infrastructure.PG)

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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockUnderlyingConn := &MockConn{beginResult: MockTx{}}
	mockPluginManager, mockPluginService := setupMockPluginManagerAndService(ctrl, mockUnderlyingConn)
	mockPluginService.EXPECT().SetCurrentTx(gomock.Any()).AnyTimes()
	mockAwsWrapperConn := *awsDriver.NewAwsWrapperConn(mockPluginManager, mockPluginService, driver_infrastructure.PG)

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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockUnderlyingConn := &MockConn{prepareResult: MockStmt{}}
	mockPluginManager, mockPluginService := setupMockPluginManagerAndService(ctrl, mockUnderlyingConn)
	mockAwsWrapperConn := *awsDriver.NewAwsWrapperConn(mockPluginManager, mockPluginService, driver_infrastructure.PG)

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

func TestMethodInvokedOnOldConnection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockUnderlyingConn := &MockConn{execResult: MockResult{}, beginResult: MockTx{}, prepareResult: MockStmt{}}
	mockUnderlyingConn.updateQueryRow([]string{"column"}, []driver.Value{"test"})

	// Use a variable to track the "current connection" so we can change it mid-test.
	currentConn := driver.Conn(mockUnderlyingConn)
	mockPluginManager := mock_driver_infrastructure.NewMockPluginManager(ctrl)
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockTelemetryFactory := mock_telemetry.NewMockTelemetryFactory(ctrl)
	mockTelemetryContext := mock_telemetry.NewMockTelemetryContext(ctrl)

	mockPluginService.EXPECT().GetCurrentConnection().DoAndReturn(func() driver.Conn {
		return currentConn
	}).AnyTimes()
	mockPluginService.EXPECT().UpdateState(gomock.Any(), gomock.Any()).AnyTimes()
	mockPluginService.EXPECT().SetCurrentTx(gomock.Any()).AnyTimes()
	mockPluginManager.EXPECT().GetTelemetryFactory().Return(mockTelemetryFactory).AnyTimes()
	mockTelemetryFactory.EXPECT().OpenTelemetryContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(mockTelemetryContext, context.TODO()).AnyTimes()
	mockTelemetryContext.EXPECT().SetAttribute(gomock.Any(), gomock.Any()).AnyTimes()
	mockPluginManager.EXPECT().SetTelemetryContext(gomock.Any()).AnyTimes()
	mockTelemetryContext.EXPECT().CloseContext().AnyTimes()
	mockTelemetryContext.EXPECT().SetSuccess(gomock.Any()).AnyTimes()
	mockTelemetryContext.EXPECT().SetError(gomock.Any()).AnyTimes()
	mockPluginManager.EXPECT().ReleaseResources().AnyTimes()

	mockPluginManager.EXPECT().
		Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(conn driver.Conn, methodName string, execFunc driver_infrastructure.ExecuteFunc, args ...any) (any, any, bool, error) {
			if conn != currentConn && !strings.Contains(methodName, "Close") {
				return nil, nil, false, errors.New("The internal connection has changed since " + methodName)
			}
			return execFunc()
		}).AnyTimes()

	mockAwsWrapperConn := *awsDriver.NewAwsWrapperConn(mockPluginManager, mockPluginService, driver_infrastructure.PG)

	rows, err := mockAwsWrapperConn.QueryContext(context.Background(), "", nil)
	if err != nil || rows.Columns()[0] != "column" {
		t.Errorf("An AWS Wrapper Conn with an underlying connection that does support QueryContext should return a result.")
	}

	res, err := mockAwsWrapperConn.ExecContext(context.Background(), "", nil)
	if err != nil || res == nil {
		t.Errorf("An AWS Wrapper Conn with an underlying connection that does support ExecContext should return a result.")
	}

	tx, err := mockAwsWrapperConn.Begin()
	if err != nil || tx == nil {
		t.Errorf("An AWS Wrapper Conn with an underlying connection should return a result to Begin.")
	}

	stmt, err := mockAwsWrapperConn.Prepare("")
	if err != nil || stmt == nil {
		t.Errorf("An AWS Wrapper Conn with an underlying connection should return a result to Prepare.")
	}

	// Switch the current connection to a different one
	differentConn := &MockConn{}
	currentConn = differentConn

	err = rows.Next([]driver.Value{})
	if err == nil || !strings.Contains(err.Error(), "The internal connection has changed since") {
		t.Errorf("After internal connection has changed, methods on Rows should fail to execute.")
	}

	_, err = res.RowsAffected()
	if err == nil || !strings.Contains(err.Error(), "The internal connection has changed since") {
		t.Errorf("After internal connection has changed, methods on Result should fail to execute.")
	}

	err = tx.Commit()
	if err == nil || !strings.Contains(err.Error(), "The internal connection has changed since") {
		t.Errorf("After internal connection has changed, methods on Tx should fail to execute.")
	}

	//nolint:all
	_, err = stmt.Exec(nil)
	if err == nil || !strings.Contains(err.Error(), "The internal connection has changed since") {
		t.Errorf("After internal connection has changed, methods on Stmt should fail to execute.")
	}

	// Closing methods should always execute, even if internal connection has changed.
	err = rows.Close()
	assert.Nil(t, err)
	err = stmt.Close()
	assert.Nil(t, err)
	err = mockAwsWrapperConn.Close()
	assert.Nil(t, err)
}

func TestWrapperDriverOpen_ParseError(t *testing.T) {
	newAwsDriver := &awsDriver.AwsWrapperDriver{}
	_, err := newAwsDriver.Open("parseError")

	// parse error
	assert.Error(t, err)
}

func TestAwsWrapperDriver_Open_WorkingDsn(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDriver := mock_database_sql_driver.NewMockDriver(ctrl)
	mockConn := mock_database_sql_driver.NewMockConn(ctrl)

	mockDriver.
		EXPECT().Open(gomock.Any()).Return(mockConn, nil)

	newAwsDriver := &awsDriver.AwsWrapperDriver{
		DriverDialect:    mysql_driver.MySQLDriverDialect{},
		UnderlyingDriver: mockDriver,
	}
	wrapperConn, err := newAwsDriver.Open("user=someuser host=somehost port=5432 database=postgres")

	assert.NoError(t, err)
	assert.NotNil(t, wrapperConn)
}

func TestAwsWrapperDriver_Open_ConnectionError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDriver := mock_database_sql_driver.NewMockDriver(ctrl)
	mockConn := mock_database_sql_driver.NewMockConn(ctrl)

	mockDriver.
		EXPECT().Open(gomock.Any()).Return(mockConn, errors.New("connect-error"))

	newAwsDriver := &awsDriver.AwsWrapperDriver{
		DriverDialect:    mysql_driver.NewMySQLDriverDialect(),
		UnderlyingDriver: mockDriver,
	}
	wrapperConn, err := newAwsDriver.Open("user=someuser host=somehost port=5432 database=postgres")

	assert.Error(t, err)
	assert.Nil(t, wrapperConn)
}

func setupmocks_awsWrapperConn_executeWithPlugins(ctrl *gomock.Controller, results ...any) (
	*mock_driver_infrastructure.MockPluginManager, *mock_driver_infrastructure.MockPluginService) {
	mockPluginManager := mock_driver_infrastructure.NewMockPluginManager(ctrl)
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockTelemetryFactory := mock_telemetry.NewMockTelemetryFactory(ctrl)
	mockTelemetryContext := mock_telemetry.NewMockTelemetryContext(ctrl)

	mockPluginService.EXPECT().GetCurrentConnection().Return(mockConn).AnyTimes()
	mockPluginService.EXPECT().UpdateState(gomock.Any(), gomock.Any()).AnyTimes()
	mockPluginManager.EXPECT().GetTelemetryFactory().Return(mockTelemetryFactory).AnyTimes()
	mockTelemetryFactory.EXPECT().OpenTelemetryContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(mockTelemetryContext, context.TODO()).AnyTimes()
	mockTelemetryContext.EXPECT().SetAttribute(gomock.Any(), gomock.Any()).AnyTimes()
	mockPluginManager.EXPECT().SetTelemetryContext(gomock.Any()).AnyTimes()
	mockTelemetryContext.EXPECT().CloseContext().AnyTimes()
	mockTelemetryContext.EXPECT().SetSuccess(gomock.Any()).AnyTimes()

	for _, result := range results {
		mockPluginManager.
			EXPECT().
			Execute(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(result, nil, true, nil)
	}

	return mockPluginManager, mockPluginService
}

func TestAwsWrapperConn_Prepare(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDriverStmt := mock_database_sql_driver.NewMockStmt(ctrl)
	mockPluginManager, mockPluginService := setupmocks_awsWrapperConn_executeWithPlugins(ctrl, mockDriverStmt)
	dbEngine := driver_infrastructure.PG

	awsWrapperconn := awsDriver.NewAwsWrapperConn(mockPluginManager, mockPluginService, dbEngine)

	driverStmt, err := awsWrapperconn.Prepare("")

	assert.NoError(t, err)
	assert.IsType(t, &awsDriver.AwsWrapperStmt{}, driverStmt)
}

func TestAwsWrapperConn_PrepareContext(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDriverStmt := mock_database_sql_driver.NewMockStmt(ctrl)
	mockPluginManager, mockPluginService := setupmocks_awsWrapperConn_executeWithPlugins(ctrl, mockDriverStmt)
	dbEngine := driver_infrastructure.PG

	awsWrapperconn := awsDriver.NewAwsWrapperConn(mockPluginManager, mockPluginService, dbEngine)

	driverStmt, err := awsWrapperconn.PrepareContext(context.TODO(), "")

	assert.NoError(t, err)
	assert.IsType(t, &awsDriver.AwsWrapperStmt{}, driverStmt)
}

func TestAwsWrapperConn_Close(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDriverStmt := mock_database_sql_driver.NewMockStmt(ctrl)
	mockPluginManager, mockPluginService := setupmocks_awsWrapperConn_executeWithPlugins(ctrl, mockDriverStmt)
	dbEngine := driver_infrastructure.PG
	mockPluginManager.EXPECT().ReleaseResources()

	awsWrapperconn := awsDriver.NewAwsWrapperConn(mockPluginManager, mockPluginService, dbEngine)

	err := awsWrapperconn.Close()
	assert.NoError(t, err)
}

func TestAwsWrapperConn_Begin(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDriverTx := mock_database_sql_driver.NewMockTx(ctrl)
	mockPluginManager, mockPluginService := setupmocks_awsWrapperConn_executeWithPlugins(ctrl, mockDriverTx)
	dbEngine := driver_infrastructure.PG
	mockPluginService.EXPECT().SetCurrentTx(gomock.Any())

	awsWrapperconn := awsDriver.NewAwsWrapperConn(mockPluginManager, mockPluginService, dbEngine)

	tx, err := awsWrapperconn.Begin()
	assert.NoError(t, err)
	assert.IsType(t, &awsDriver.AwsWrapperTx{}, tx)
}

func TestAwsWrapperConn_BeginTx(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDriverTx := mock_database_sql_driver.NewMockTx(ctrl)
	mockDriverResults := mock_database_sql_driver.NewMockResult(ctrl)
	mockPluginManager, mockPluginService := setupmocks_awsWrapperConn_executeWithPlugins(ctrl, mockDriverResults, mockDriverTx)
	dbEngine := driver_infrastructure.PG
	mockPluginService.EXPECT().SetCurrentTx(gomock.Any())

	awsWrapperconn := awsDriver.NewAwsWrapperConn(mockPluginManager, mockPluginService, dbEngine)

	tx, err := awsWrapperconn.BeginTx(context.TODO(), driver.TxOptions{})
	assert.NoError(t, err)
	assert.IsType(t, &awsDriver.AwsWrapperTx{}, tx)
}

func TestAwsWrapperConn_QueryContext(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDriverRows := mock_database_sql_driver.NewMockRows(ctrl)
	mockPluginManager, mockPluginService := setupmocks_awsWrapperConn_executeWithPlugins(ctrl, mockDriverRows)
	dbEngine := driver_infrastructure.PG

	awsWrapperconn := awsDriver.NewAwsWrapperConn(mockPluginManager, mockPluginService, dbEngine)

	tx, err := awsWrapperconn.QueryContext(context.TODO(), "", nil)
	assert.NoError(t, err)
	assert.IsType(t, &awsDriver.AwsWrapperPgRows{}, tx)
}

func TestAwsWrapperConn_ExecContext(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDriverResults := mock_database_sql_driver.NewMockResult(ctrl)
	mockPluginManager, mockPluginService := setupmocks_awsWrapperConn_executeWithPlugins(ctrl, mockDriverResults)
	dbEngine := driver_infrastructure.PG

	awsWrapperconn := awsDriver.NewAwsWrapperConn(mockPluginManager, mockPluginService, dbEngine)

	tx, err := awsWrapperconn.ExecContext(context.TODO(), "", nil)
	assert.NoError(t, err)
	assert.IsType(t, &awsDriver.AwsWrapperResult{}, tx)
}

func TestAwsWrapperConn_Ping(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDriverResults := mock_database_sql_driver.NewMockResult(ctrl)
	mockPluginManager, mockPluginService := setupmocks_awsWrapperConn_executeWithPlugins(ctrl, mockDriverResults)
	dbEngine := driver_infrastructure.PG

	awsWrapperconn := awsDriver.NewAwsWrapperConn(mockPluginManager, mockPluginService, dbEngine)

	err := awsWrapperconn.Ping(context.TODO())
	assert.NoError(t, err)
}

func TestAwsWrapperConn_IsValid(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDriverResults := mock_database_sql_driver.NewMockResult(ctrl)
	mockPluginManager, mockPluginService := setupmocks_awsWrapperConn_executeWithPlugins(ctrl, mockDriverResults)
	dbEngine := driver_infrastructure.PG

	awsWrapperconn := awsDriver.NewAwsWrapperConn(mockPluginManager, mockPluginService, dbEngine)

	isValid := awsWrapperconn.IsValid()
	assert.True(t, isValid)
}

func TestAwsWrapperConn_ResetSession(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSessionResetter := mock_database_sql_driver.NewMockSessionResetter(ctrl)
	mockPluginManager, mockPluginService := setupmocks_awsWrapperConn_executeWithPlugins(ctrl, mockSessionResetter)
	mockPluginService.EXPECT().ResetSession()
	dbEngine := driver_infrastructure.PG

	awsWrapperconn := awsDriver.NewAwsWrapperConn(mockPluginManager, mockPluginService, dbEngine)

	err := awsWrapperconn.ResetSession(context.TODO())
	assert.NoError(t, err)
}

func TestAwsWrapperConn_CheckedNamedValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockNamedValueChecker := mock_database_sql_driver.NewMockNamedValueChecker(ctrl)
	mockPluginManager, mockPluginService := setupmocks_awsWrapperConn_executeWithPlugins(ctrl, mockNamedValueChecker)
	dbEngine := driver_infrastructure.PG

	awsWrapperconn := awsDriver.NewAwsWrapperConn(mockPluginManager, mockPluginService, dbEngine)

	err := awsWrapperconn.CheckNamedValue(nil)
	assert.NoError(t, err)
}
