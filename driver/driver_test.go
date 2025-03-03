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
	"strings"
	"testing"
)

func TestAwsWrapperError(t *testing.T) {
	testError := NewUnavailableHostError("test")
	if testError.IsFailoverErrorType() {
		t.Errorf("Should return false, UnavailableHostError is not a failover error type.")
	}
	if !testError.IsType(UnavailableHostErrorType) {
		t.Errorf("Should return true, error is a UnavailableHostErrorType.")
	}
	if testError.IsType(UnsupportedMethodErrorType) {
		t.Errorf("Should return false, error is not a UnsupportedMethodErrorType.")
	}
	if !strings.Contains(testError.Error(), "test") {
		t.Errorf("Error should include 'test', improperly handles message.")
	}

	if !FailoverSuccessError.IsFailoverErrorType() {
		t.Errorf("Should return true, FailoverSuccessError is a failover error type.")
	}
	if !FailoverSuccessError.IsType(FailoverSuccessErrorType) {
		t.Errorf("Should return true, error is a FailoverSuccessErrorType.")
	}
	if FailoverSuccessError.IsType(UnsupportedMethodErrorType) {
		t.Errorf("Should return false, error is not a UnsupportedMethodErrorType.")
	}
	if FailoverSuccessError.Error() != GetMessage("Failover.connectionChangedError") {
		t.Errorf("Should return message attached to Failover.connectionChangedError, improperly handles message.")
	}
}

func TestIsDialectPg(t *testing.T) {
	testConn := MockDriverConn{nil}
	pgDialect := PgDatabaseDialect{}
	implementsQueryer := pgDialect.IsDialect(testConn)
	if implementsQueryer {
		t.Errorf("Should return false, connection does not implement QueryContext.")
	}
	testConnWithQuery := MockConn{nil, nil, nil, nil, true}
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

	rdsPgDialect := RdsPgDatabaseDialect{}

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

	auroraPgDialect := AuroraPgDatabaseDialect{}

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
	mySqlDialect := MySQLDatabaseDialect{}
	testConnWithQuery := MockConn{nil, nil, nil, nil, true}

	testConnWithQuery.updateQueryRow([]string{"variable_name"}, []driver.Value{"version_comment"})
	returnsRow := mySqlDialect.IsDialect(testConnWithQuery)
	if returnsRow {
		t.Errorf("Should return false as needed value is out of range.")
	}

	testConnWithQuery.updateQueryRow([]string{"variable_name", "value"}, []driver.Value{"version_comment", []uint8{109, 121, 115, 113, 108}})
	returnsRow = mySqlDialect.IsDialect(testConnWithQuery)
	if returnsRow {
		t.Errorf("Should return false as query result does not contains 'MySQL'.")
	}

	testConnWithQuery.updateQueryRow([]string{"variable_name", "value"}, []driver.Value{"version_comment", []uint8{77, 121, 83, 81, 76}})
	returnsRow = mySqlDialect.IsDialect(testConnWithQuery)
	if !returnsRow {
		t.Errorf("Should return true as query result contains 'MySQL'.")
	}

	rdsMySQLDialect := RdsMySQLDatabaseDialect{}

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

	testConnWithQuery.updateQueryRow([]string{"variable_name", "value"}, []driver.Value{"version_comment", []uint8{83, 111, 117, 114, 99, 101, 32, 100, 105, 115, 116, 114, 105, 98, 117, 116, 105, 111, 110}})
	returnsRow = rdsMySQLDialect.IsDialect(testConnWithQuery)
	if !returnsRow {
		t.Errorf("Should return true as query result contains 'Source Distribution'.")
	}

	auroraMySQLDialect := AuroraMySQLDatabaseDialect{}

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
	mockPluginManager := &ConnectionPluginManager{targetDriver: nil}

	baseAwsWrapperConn := AwsWrapperConn{underlyingConn: MockDriverConn{}, pluginManager: mockPluginManager, engine: MYSQL}
	res, err := baseAwsWrapperConn.QueryContext(context.Background(), "", nil)
	if res != nil || !strings.Contains(err.Error(), "The underlying driver connection does not implement the required interface 'driver.QueryerContext'.") {
		t.Errorf("An AWS Wrapper Conn with an underlying connection that does not support QueryContext should not return a result.")
	}

	mockUnderlyingConn := MockConn{nil, nil, nil, nil, false}
	mockUnderlyingConn.updateQueryRow([]string{"column"}, []driver.Value{"test"})
	mockAwsWrapperConn := AwsWrapperConn{underlyingConn: mockUnderlyingConn, pluginManager: mockPluginManager, engine: MYSQL}
	res, err = mockAwsWrapperConn.QueryContext(context.Background(), "", nil)
	if err != nil || res.Columns()[0] != "column" {
		t.Errorf("An AWS Wrapper Conn with an underlying connection that does support QueryContext should return a result.")
	}

	mysqlRows, ok := res.(*AwsWrapperMySQLRows)
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
	mockPluginManager := &ConnectionPluginManager{targetDriver: nil}

	baseAwsWrapperConn := AwsWrapperConn{underlyingConn: MockDriverConn{}, pluginManager: mockPluginManager, engine: PG}
	res, err := baseAwsWrapperConn.QueryContext(context.Background(), "", nil)
	if res != nil || !strings.Contains(err.Error(), "The underlying driver connection does not implement the required interface 'driver.QueryerContext'.") {
		t.Errorf("An AWS Wrapper Conn with an underlying connection that does not support QueryContext should not return a result.")
	}

	mockUnderlyingConn := MockConn{nil, nil, nil, nil, false}
	mockUnderlyingConn.updateQueryRow([]string{"column"}, []driver.Value{"test"})
	mockAwsWrapperConn := AwsWrapperConn{underlyingConn: mockUnderlyingConn, pluginManager: mockPluginManager, engine: PG}
	res, err = mockAwsWrapperConn.QueryContext(context.Background(), "", nil)
	if err != nil || res.Columns()[0] != "column" {
		t.Errorf("An AWS Wrapper Conn with an underlying connection that does support QueryContext should return a result.")
	}

	pgRows, ok := res.(*AwsWrapperPgRows)
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
	mockPluginManager := &ConnectionPluginManager{targetDriver: nil}

	mockUnderlyingConn := MockConn{nil, MockResult{}, nil, nil, false}
	mockAwsWrapperConn := AwsWrapperConn{underlyingConn: mockUnderlyingConn, pluginManager: mockPluginManager, engine: PG}

	res, err := mockAwsWrapperConn.ExecContext(context.Background(), "", nil)
	if err != nil || res == nil {
		t.Errorf("An AWS Wrapper Conn with an underlying connection that does support ExecContext should return a result.")
	}

	awsResult, ok := res.(*AwsWrapperResult)
	if ok == false {
		t.Errorf("Wrapped ExecContext should return type of AwsWrapperResult.")
	}

	num, err := awsResult.RowsAffected()
	if num != -1 && !strings.Contains(err.Error(), "MockResult") {
		t.Errorf("Wrapped RowsAffected should return results of operations on underlyingResult.")
	}
}

func TestWrapperUtilsBeginWithPlugins(t *testing.T) {
	mockPluginManager := &ConnectionPluginManager{targetDriver: nil}

	mockUnderlyingConn := MockConn{nil, nil, MockTx{}, nil, false}
	mockAwsWrapperConn := AwsWrapperConn{underlyingConn: mockUnderlyingConn, pluginManager: mockPluginManager, engine: PG}

	tx, err := mockAwsWrapperConn.Begin()
	if err != nil || tx == nil {
		t.Errorf("An AWS Wrapper Conn with an underlying connection should return a result to Begin.")
	}

	awsTx, ok := tx.(*AwsWrapperTx)
	if ok == false {
		t.Errorf("Wrapped Begin should return type of AwsWrapperTx.")
	}

	err = awsTx.Commit()
	if err == nil || !strings.Contains(err.Error(), "MockTx") {
		t.Errorf("Wrapped Commit should return results of operations on underlyingTx.")
	}
}

func TestWrapperUtilsPrepareWithPlugins(t *testing.T) {
	mockPluginManager := &ConnectionPluginManager{targetDriver: nil}

	mockUnderlyingConn := MockConn{nil, nil, nil, MockStmt{}, false}
	mockAwsWrapperConn := AwsWrapperConn{underlyingConn: mockUnderlyingConn, pluginManager: mockPluginManager, engine: PG}

	res, err := mockAwsWrapperConn.Prepare("")
	if err != nil || res == nil {
		t.Errorf("An AWS Wrapper Conn with an underlying connection should return a result to Prepare.")
	}

	awsStmt, ok := res.(*AwsWrapperStmt)
	if ok == false {
		t.Errorf("Wrapped Prepare should return type of AwsWrapperStmt.")
	}

	val, err := awsStmt.Exec(nil)
	if val != nil || !strings.Contains(err.Error(), "MockStmt") {
		t.Errorf("Wrapped Exec should return results of operations on underlyingStmt.")
	}

	val, err = awsStmt.ExecContext(context.Background(), nil)
	if val != nil || !strings.Contains(err.Error(), "The underlying driver statement does not implement the required interface 'driver.StmtExecContext'.") {
		t.Errorf("The returned stmt should attempt optional interfaces that are supported by both pgx and mysql drivers.")
	}
}

type MockConn struct {
	queryResult   driver.Rows
	execResult    driver.Result
	beginResult   driver.Tx
	prepareResult driver.Stmt
	throwError    bool
}

func (m MockConn) Close() error {
	return nil
}

func (m MockConn) Prepare(query string) (driver.Stmt, error) {
	return m.prepareResult, nil
}

func (m MockConn) Begin() (driver.Tx, error) {
	return m.beginResult, nil
}

func (m MockConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if m.queryResult != nil || m.throwError == false {
		return m.queryResult, nil
	}
	return nil, errors.New("test error")
}

func (m MockConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return m.execResult, nil
}

func (m *MockConn) updateQueryRow(columns []string, row []driver.Value) {
	testRow := MockRows{columns: columns, row: row}
	m.queryResult = testRow
}

func (m *MockConn) updateThrowError(throwError bool) {
	m.throwError = throwError
}

type MockStmt struct {
}

func (a MockStmt) Close() error {
	return errors.New("MockStmt error")
}

func (a MockStmt) Exec(args []driver.Value) (driver.Result, error) {
	return MockResult{}, errors.New("MockStmt error")
}

func (a MockStmt) NumInput() int {
	return 1
}

func (a MockStmt) Query(args []driver.Value) (driver.Rows, error) {
	return MockRows{[]string{"column"}, []driver.Value{"test"}}, nil
}

type MockResult struct {
}

func (m MockResult) LastInsertId() (int64, error) {
	return 1, errors.New("MockResult error")
}

func (m MockResult) RowsAffected() (int64, error) {
	return 1, errors.New("MockResult error")
}

type MockTx struct {
}

func (a MockTx) Commit() error {
	return errors.New("MockTx error")
}

func (a MockTx) Rollback() error {
	return errors.New("MockTx error")
}

type MockDriverConn struct {
	driver.Conn
}

type MockRows struct {
	columns []string
	row     []driver.Value
}

func (t MockRows) Columns() []string {
	return t.columns
}

func (t MockRows) Close() error {
	return nil
}

func (t MockRows) Next(dest []driver.Value) error {
	if len(t.row) < 1 {
		return errors.New("test error")
	}
	for i := range dest {
		dest[i] = t.row[i]
	}
	return nil
}
