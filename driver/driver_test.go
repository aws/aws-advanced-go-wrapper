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

package main

import (
	awsDriver "awssql/driver"
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
)

func TestDummy(t *testing.T) {
	got := 1
	want := 1

	if got != want {
		t.Errorf("got %q, wanted %q", got, want)
	}
}

func TestImplementations(t *testing.T) {
	// Check for correct implementations of interfaces on left.
	var _ error = (*awsDriver.AwsWrapperError)(nil)
	var _ awsDriver.DatabaseDialect = (*awsDriver.MySQLDatabaseDialect)(nil)
	var _ awsDriver.DatabaseDialect = (*awsDriver.RdsMySQLDatabaseDialect)(nil)
	var _ awsDriver.DatabaseDialect = (*awsDriver.AuroraMySQLDatabaseDialect)(nil)
	var _ awsDriver.DatabaseDialect = (*awsDriver.PgDatabaseDialect)(nil)
	var _ awsDriver.DatabaseDialect = (*awsDriver.RdsPgDatabaseDialect)(nil)
	var _ awsDriver.DatabaseDialect = (*awsDriver.AuroraPgDatabaseDialect)(nil)
	var _ awsDriver.DialectProvider = (*awsDriver.DialectManager)(nil)
	var _ awsDriver.PluginManager = (*awsDriver.ConnectionPluginManager)(nil)
	var _ awsDriver.PluginManager = (*awsDriver.ConnectionPluginManager)(nil)
	var _ awsDriver.PluginService = (*awsDriver.PluginServiceImpl)(nil)
	var _ driver.Driver = (*awsDriver.AwsWrapperDriver)(nil)
	var _ driver.Conn = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.Pinger = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.ExecerContext = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.QueryerContext = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.ConnPrepareContext = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.ConnBeginTx = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.SessionResetter = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.Validator = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.Stmt = (*awsDriver.AwsWrapperStmt)(nil)
	var _ driver.StmtExecContext = (*awsDriver.AwsWrapperStmt)(nil)
	var _ driver.StmtQueryContext = (*awsDriver.AwsWrapperStmt)(nil)
}

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
	pgDialect := awsDriver.PgDatabaseDialect{}
	implementsQueryer := pgDialect.IsDialect(testConn)
	if implementsQueryer {
		t.Errorf("Should return false, connection does not implement QueryContext.")
	}
	testConnWithQuery := MockConn{nil, true}
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

	rdsPgDialect := awsDriver.RdsPgDatabaseDialect{}

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

	auroraPgDialect := awsDriver.AuroraPgDatabaseDialect{}

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
	mySqlDialect := awsDriver.MySQLDatabaseDialect{}
	testConnWithQuery := MockConn{nil, true}

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

	rdsMySQLDialect := awsDriver.RdsMySQLDatabaseDialect{}

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

	auroraMySQLDialect := awsDriver.AuroraMySQLDatabaseDialect{}

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

type MockConn struct {
	queryRow   driver.Rows
	throwError bool
}

func (m MockConn) Close() error {
	return nil
}

func (m MockConn) Prepare(query string) (driver.Stmt, error) {
	return nil, nil
}

func (m MockConn) Begin() (driver.Tx, error) {
	return nil, nil
}

func (m MockConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if m.queryRow != nil || m.throwError == false {
		return m.queryRow, nil
	}
	return nil, errors.New("test error")
}

func (m *MockConn) updateQueryRow(columns []string, row []driver.Value) {
	testRow := MockRows{columns: columns, row: row}
	m.queryRow = testRow
}

func (m *MockConn) updateThrowError(throwError bool) {
	m.throwError = throwError
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
