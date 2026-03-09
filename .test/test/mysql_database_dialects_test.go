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
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"

	mock_database_sql_driver "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/database_sql_driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestMySQLDatabaseDialect_GetDialectUpdateCandidates(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.MySQLDatabaseDialect{}

	expectedCandidates := []string{
		driver_infrastructure.GLOBAL_AURORA_MYSQL_DIALECT,
		driver_infrastructure.AURORA_MYSQL_DIALECT,
		driver_infrastructure.RDS_MYSQL_MULTI_AZ_CLUSTER_DIALECT,
		driver_infrastructure.RDS_MYSQL_DIALECT}
	assert.ElementsMatch(t, expectedCandidates, testDatabaseDialect.GetDialectUpdateCandidates())
}

func TestMySQLDatabaseDialect_GetDefaultPort(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.MySQLDatabaseDialect{}
	expectedDefaultPort := 3306
	assert.Equal(t, testDatabaseDialect.GetDefaultPort(), expectedDefaultPort)
}

func TestMySQLDatabaseDialect_GetHostAlias(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.MySQLDatabaseDialect{}
	expectedHostAliasQuery := "SELECT CONCAT(@@hostname, ':', @@port)"

	assert.Equal(t, expectedHostAliasQuery, testDatabaseDialect.GetHostAliasQuery())
}

func TestMySQLDatabaseDialect_GetServerVersion(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.MySQLDatabaseDialect{}
	expectedGetServerVersionQuery := "SHOW VARIABLES LIKE 'version_comment'"
	assert.Equal(t, expectedGetServerVersionQuery, testDatabaseDialect.GetServerVersionQuery())
}

func TestMySQLDatabaseDialect(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.MySQLDatabaseDialect{}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockQueryer := mock_database_sql_driver.NewMockQueryerContext(ctrl)
	mockRows := mock_database_sql_driver.NewMockRows(ctrl)

	conn := struct {
		driver.Conn
		driver.QueryerContext
	}{
		Conn:           mockConn,
		QueryerContext: mockQueryer,
	}

	expectedIsDialectQuery := "SHOW VARIABLES LIKE 'version_comment'"

	// IsDialect - true
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)

	mockRows.EXPECT().Columns().Return([]string{"version_comment", "MySQL"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = "version_comment"
		dest[1] = "MySQL"
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	assert.True(t, testDatabaseDialect.IsDialect(conn), "Expected MySQLDatabaseDialect.IsDialect to be true")

	// IsDialect - false
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)

	mockRows.EXPECT().Columns().Return([]string{})
	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrBadConn)
	mockRows.EXPECT().Close().Return(nil)

	assert.False(t, testDatabaseDialect.IsDialect(conn), "Expected MySQLDatabaseDialect.IsDialect to be false")
}

func TestMySQLDatabaseDialect_GetHostListProvider(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.MySQLDatabaseDialect{}
	supplier := testDatabaseDialect.GetHostListProviderSupplier()
	assert.NotNil(t, supplier, "expected a non-nil HostListProviderSupplier")
}

func TestRdsMySQLDatabaseDialect_GetDialectUpdateCandidates(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMySQLDatabaseDialect{}
	expectedCandidates := []string{
		driver_infrastructure.RDS_MYSQL_MULTI_AZ_CLUSTER_DIALECT,
		driver_infrastructure.GLOBAL_AURORA_MYSQL_DIALECT,
		driver_infrastructure.AURORA_MYSQL_DIALECT}

	assert.ElementsMatch(t, expectedCandidates, testDatabaseDialect.GetDialectUpdateCandidates())
}

func TestRdsMySQLDatabaseDialect_GetDefaultPort(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMySQLDatabaseDialect{}
	expectedDefaultPort := 3306

	assert.Equal(t, testDatabaseDialect.GetDefaultPort(), expectedDefaultPort)
}

func TestRdsMySQLDatabaseDialect_GetHostAliasQuery(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMySQLDatabaseDialect{}
	expectedHostAliasQuery := "SELECT CONCAT(@@hostname, ':', @@port)"

	assert.Equal(t, expectedHostAliasQuery, testDatabaseDialect.GetHostAliasQuery())
}

func TestRdsMySQLDatabaseDialect_GetServerVersion(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMySQLDatabaseDialect{}
	expectedGetServerVersionQuery := "SHOW VARIABLES LIKE 'version_comment'"

	assert.Equal(t, expectedGetServerVersionQuery, testDatabaseDialect.GetServerVersionQuery())
}

func TestRdsMySQLDatabaseDialect_IsDialect(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMySQLDatabaseDialect{}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockQueryer := mock_database_sql_driver.NewMockQueryerContext(ctrl)
	mockRows := mock_database_sql_driver.NewMockRows(ctrl)

	conn := struct {
		driver.Conn
		driver.QueryerContext
	}{
		Conn:           mockConn,
		QueryerContext: mockQueryer,
	}

	expectedIsDialectQuery := "SHOW VARIABLES LIKE 'version_comment'"

	// IsDialect - true
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)

	mockRows.EXPECT().Columns().Return([]string{"version_comment", "Source distribution"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = "version_comment"
		dest[1] = "Source distribution"
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	assert.True(t, testDatabaseDialect.IsDialect(conn), "Expected RdsMySQLDatabaseDialect.IsDialect to be true")

	// IsDialect - false
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)

	mockRows.EXPECT().Columns().Return([]string{})
	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrBadConn)
	mockRows.EXPECT().Close().Return(nil)

	assert.False(t, testDatabaseDialect.IsDialect(conn), "Expected RdsMySQLDatabaseDialect.IsDialect to be false")
}

func TestRdsMySQLDatabaseDialect_GetHostListProvider(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMySQLDatabaseDialect{}
	supplier := testDatabaseDialect.GetHostListProviderSupplier()
	assert.NotNil(t, supplier, "expected a non-nil HostListProviderSupplier")
}

func TestAuroraRdsMySQLDatabaseDialect_GetDialectUpdateCandidates(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraMySQLDatabaseDialect{}
	expectedCandidates := []string{
		driver_infrastructure.GLOBAL_AURORA_MYSQL_DIALECT,
		driver_infrastructure.RDS_MYSQL_MULTI_AZ_CLUSTER_DIALECT,
	}

	assert.ElementsMatch(t, expectedCandidates, testDatabaseDialect.GetDialectUpdateCandidates())
}

func TestAuroraRdsMySQLDatabaseDialect_GetDefaultPort(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraMySQLDatabaseDialect{}
	expectedDefaultPort := 3306

	assert.Equal(t, testDatabaseDialect.GetDefaultPort(), expectedDefaultPort)
}

func TestAuroraRdsMySQLDatabaseDialect_GetHostAliasQuery(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraMySQLDatabaseDialect{}
	expectedHostAliasQuery := "SELECT CONCAT(@@hostname, ':', @@port)"

	assert.Equal(t, expectedHostAliasQuery, testDatabaseDialect.GetHostAliasQuery())
}

func TestAuroraRdsMySQLDatabaseDialect_GetServerVersion(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraMySQLDatabaseDialect{}
	expectedGetServerVersionQuery := "SHOW VARIABLES LIKE 'version_comment'"

	assert.Equal(t, expectedGetServerVersionQuery, testDatabaseDialect.GetServerVersionQuery())
}

func TestAuroraRdsMySQLDatabaseDialect_IsDialect(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraMySQLDatabaseDialect{}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockQueryer := mock_database_sql_driver.NewMockQueryerContext(ctrl)
	mockRows := mock_database_sql_driver.NewMockRows(ctrl)

	conn := struct {
		driver.Conn
		driver.QueryerContext
	}{
		Conn:           mockConn,
		QueryerContext: mockQueryer,
	}

	expectedIsDialectQuery := "SHOW VARIABLES LIKE 'aurora_version'"

	// IsDialect - true
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)

	mockRows.EXPECT().Columns().Return([]string{"version_comment", "Source distribution"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = ""
		dest[1] = ""
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	assert.True(t, testDatabaseDialect.IsDialect(conn), "Expected AuroraMySQLDatabaseDialect.IsDialect to be true")

	// IsDialect - false
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)

	mockRows.EXPECT().Columns().Return([]string{})
	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrBadConn)
	mockRows.EXPECT().Close().Return(nil)

	assert.False(t, testDatabaseDialect.IsDialect(conn), "Expected AuroraMySQLDatabaseDialect.IsDialect to be false")
}

func TestAuroraRdsMySQLDatabaseDialect_GetHostListProvider(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraMySQLDatabaseDialect{}
	supplier := testDatabaseDialect.GetHostListProviderSupplier()
	assert.NotNil(t, supplier, "expected a non-nil HostListProviderSupplier")
}

func TestAuroraRdsMySQLDatabaseDialect_GetTopologyQuery(t *testing.T) {
	dialect := &driver_infrastructure.AuroraMySQLDatabaseDialect{}
	expected := "SELECT server_id, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END as is_writer, " +
		"cpu, REPLICA_LAG_IN_MILLISECONDS as 'lag', LAST_UPDATE_TIMESTAMP as last_update_timestamp " +
		"FROM information_schema.replica_host_status " +
		"WHERE time_to_sec(timediff(now(), LAST_UPDATE_TIMESTAMP)) <= 300 OR SESSION_ID = 'MASTER_SESSION_ID' "
	assert.Equal(t, expected, dialect.GetTopologyQuery())
}

func TestAuroraRdsMySQLDatabaseDialect_GetInstanceIdQuery(t *testing.T) {
	dialect := &driver_infrastructure.AuroraMySQLDatabaseDialect{}
	assert.Equal(t, "SELECT @@aurora_server_id, @@aurora_server_id;", dialect.GetInstanceIdQuery())
}

func TestAuroraRdsMySQLDatabaseDialect_GetWriterIdQuery(t *testing.T) {
	dialect := &driver_infrastructure.AuroraMySQLDatabaseDialect{}
	expected := "SELECT server_id " +
		"FROM information_schema.replica_host_status " +
		"WHERE SESSION_ID = 'MASTER_SESSION_ID' AND SERVER_ID = @@aurora_server_id"
	assert.Equal(t, expected, dialect.GetWriterIdQuery())
}

func TestAuroraRdsMySQLDatabaseDialect_GetIsReaderQuery(t *testing.T) {
	dialect := &driver_infrastructure.AuroraMySQLDatabaseDialect{}
	assert.Equal(t, "SELECT @@innodb_read_only", dialect.GetIsReaderQuery())
}

func TestRdsMultiAzClusterMySQLDatabaseDialect_GetDialectUpdateCandidates(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMultiAzClusterMySQLDatabaseDialect{}
	var expectedCandidates []string

	assert.ElementsMatch(t, expectedCandidates, testDatabaseDialect.GetDialectUpdateCandidates())
}

func TestRdsMultiAzClusterMySQLDatabaseDialect_GetDefaultPort(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMultiAzClusterMySQLDatabaseDialect{}
	expectedDefaultPort := 3306

	assert.Equal(t, testDatabaseDialect.GetDefaultPort(), expectedDefaultPort)
}

func TestRdsMultiAzClusterMySQLDatabaseDialect_GetHostAliasQuery(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMultiAzClusterMySQLDatabaseDialect{}
	expectedHostAliasQuery := "SELECT CONCAT(@@hostname, ':', @@port)"

	assert.Equal(t, expectedHostAliasQuery, testDatabaseDialect.GetHostAliasQuery())
}

func TestRdsMultiAzClusterMySQLDatabaseDialect_GetServerVersion(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMultiAzClusterMySQLDatabaseDialect{}
	expectedGetServerVersionQuery := "SHOW VARIABLES LIKE 'version_comment'"

	assert.Equal(t, expectedGetServerVersionQuery, testDatabaseDialect.GetServerVersionQuery())
}

func TestRdsMultiAzClusterMySQLDatabaseDialect_GetHostListProvider(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMultiAzClusterMySQLDatabaseDialect{}
	supplier := testDatabaseDialect.GetHostListProviderSupplier()
	assert.NotNil(t, supplier, "expected a non-nil HostListProviderSupplier")
}

func TestRdsMultiAzClusterMySQLDatabaseDialect_GetTopologyQuery(t *testing.T) {
	dialect := &driver_infrastructure.RdsMultiAzClusterMySQLDatabaseDialect{}
	assert.Equal(t, "SELECT id, endpoint FROM mysql.rds_topology", dialect.GetTopologyQuery())
}

func TestRdsMultiAzClusterMySQLDatabaseDialect_GetInstanceIdQuery(t *testing.T) {
	dialect := &driver_infrastructure.RdsMultiAzClusterMySQLDatabaseDialect{}
	expected := "SELECT id, SUBSTRING_INDEX(endpoint, '.', 1) FROM mysql.rds_topology WHERE id = @@server_id"
	assert.Equal(t, expected, dialect.GetInstanceIdQuery())
}

func TestRdsMultiAzClusterMySQLDatabaseDialect_GetWriterIdQuery(t *testing.T) {
	dialect := &driver_infrastructure.RdsMultiAzClusterMySQLDatabaseDialect{}
	assert.Equal(t, "SHOW REPLICA STATUS", dialect.GetWriterIdQuery())
}

func TestRdsMultiAzClusterMySQLDatabaseDialect_GetWriterIdColumnName(t *testing.T) {
	dialect := &driver_infrastructure.RdsMultiAzClusterMySQLDatabaseDialect{}
	assert.Equal(t, "Source_Server_Id", dialect.GetWriterIdColumnName())
}

func TestRdsMultiAzClusterMySQLDatabaseDialect_GetIsReaderQuery(t *testing.T) {
	dialect := &driver_infrastructure.RdsMultiAzClusterMySQLDatabaseDialect{}
	assert.Equal(t, "SELECT @@read_only", dialect.GetIsReaderQuery())
}

func TestMysqlDoesSetReadOnly(t *testing.T) {
	setMysqlReadOnlyTestFunc(t, " select 1 ", [][]bool{{false, false}})
	setMysqlReadOnlyTestFunc(t, " select /* COMMENT */ 1 ", [][]bool{{false, false}})
	setMysqlReadOnlyTestFunc(t, " SET session transaction read only ", [][]bool{{true, true}})
	setMysqlReadOnlyTestFunc(t, " set session transaction read /* COMMENT */ only ", [][]bool{{true, true}})
	setMysqlReadOnlyTestFunc(t, " set session transaction read /* COMMENT */ only ", [][]bool{{true, true}})
	setMysqlReadOnlyTestFunc(t, " /* COMMENT */ set session transaction read /* COMMENT */ only ", [][]bool{{true, true}})
	setMysqlReadOnlyTestFunc(t, " set session transaction read write ", [][]bool{{false, true}})
	setMysqlReadOnlyTestFunc(t, " /* COMMENT */ set session transaction /* COMMENT */ read write ", [][]bool{{false, true}})
	setMysqlReadOnlyTestFunc(t, " /* COMMENT */ set session transaction /* COMMENT */ read write ", [][]bool{{false, true}})
	setMysqlReadOnlyTestFunc(t, " set session transaction /* COMMENT */ read write ", [][]bool{{false, true}})
	setMysqlReadOnlyTestFunc(t, " set SESSION TRANSACTION read only; set session transaction read write", [][]bool{{true, true}, {false, true}})
	setMysqlReadOnlyTestFunc(t, " set session transaction read only;,  select 1", [][]bool{{true, true}, {false, false}})
	setMysqlReadOnlyTestFunc(t, " set session  /* COMMENT */transaction read only/* COMMENT */; select 1", [][]bool{{true, true}, {false, false}})
	setMysqlReadOnlyTestFunc(t, " select 1; set session transaction read only; ", [][]bool{{false, false}, {true, true}})
	setMysqlReadOnlyTestFunc(t, " set session transaction READ ONLY; set session transaction read write; ", [][]bool{{true, true}, {false, true}})
	setMysqlReadOnlyTestFunc(t, " set session transaction read write; set session transaction read only; ", [][]bool{{false, true}, {true, true}})
	setMysqlReadOnlyTestFunc(t, " set session transaction read write; select 1", [][]bool{{false, true}, {false, false}})
	setMysqlReadOnlyTestFunc(t, " select 1; set session transaction read write; select 1", [][]bool{{false, false}, {false, true}, {false, false}})
}

func setMysqlReadOnlyTestFunc(t *testing.T, query string, expectedValues [][]bool) {
	statements := utils.GetSeparateSqlStatements(query)
	dialect := &driver_infrastructure.MySQLDatabaseDialect{}
	assert.Equal(t, len(expectedValues), len(statements))

	for i, statement := range statements {
		readOnly, ok := dialect.DoesStatementSetReadOnly(statement)
		assert.Equal(t, expectedValues[i][0], readOnly)
		assert.Equal(t, expectedValues[i][1], ok)
	}
}

func TestMysqlDoesSetAutoCommit(t *testing.T) {
	setMysqlAutoCommitTestFunc(t, " select 1 ", [][]bool{{false, false}})
	setMysqlAutoCommitTestFunc(t, " select /* COMMENT */ 1 ", [][]bool{{false, false}})
	setMysqlAutoCommitTestFunc(t, " /* COMMENT */ select /* COMMENT */ 1 ", [][]bool{{false, false}})
	setMysqlAutoCommitTestFunc(t, " set autocommit = 1 ", [][]bool{{true, true}})
	setMysqlAutoCommitTestFunc(t, " set autocommit = 0 ", [][]bool{{false, true}})
	setMysqlAutoCommitTestFunc(t, " set autocommit=1 ", [][]bool{{true, true}})
	setMysqlAutoCommitTestFunc(t, " set autocommit=0 ", [][]bool{{false, true}})
	setMysqlAutoCommitTestFunc(t, " set autocommit=/* COMMENT */0  ", [][]bool{{false, true}})
	setMysqlAutoCommitTestFunc(t, " set autocommit=1; set autocommit=0 ", [][]bool{{true, true}, {false, true}})
	setMysqlAutoCommitTestFunc(t, " set autocommit=0; set autocommit=1 ", [][]bool{{false, true}, {true, true}})
}

func setMysqlAutoCommitTestFunc(t *testing.T, query string, expectedValues [][]bool) {
	statements := utils.GetSeparateSqlStatements(query)
	dialect := &driver_infrastructure.MySQLDatabaseDialect{}
	assert.Equal(t, len(expectedValues), len(statements))

	for i, statement := range statements {
		autoCommit, ok := dialect.DoesStatementSetAutoCommit(statement)
		assert.Equal(t, expectedValues[i][0], autoCommit)
		assert.Equal(t, expectedValues[i][1], ok)
	}
}

func TestMysqlDoesSetCatalog(t *testing.T) {
	setMysqlCatalogTestFunc(t, " select 1 ", [][]any{{"", false}})
	setMysqlCatalogTestFunc(t, " select /* COMMENT */ 1 ", [][]any{{"", false}})
	setMysqlCatalogTestFunc(t, " /* COMMENT */ select /* COMMENT */ 1 ", [][]any{{"", false}})
	setMysqlCatalogTestFunc(t, " USE dbName ", [][]any{{"dbName", true}})
	setMysqlCatalogTestFunc(t, " use/* COMMENT USE dbName3*/ dbName ", [][]any{{"dbName", true}})
	setMysqlCatalogTestFunc(t, " use dbName1 ; use dbName2 ", [][]any{{"dbName1", true}, {"dbName2", true}})
	setMysqlCatalogTestFunc(t, " SELECT * from user; select /* use dbName */ * from user ", [][]any{{"", false}, {"", false}})
	setMysqlCatalogTestFunc(t, " use dbName; select 1 ", [][]any{{"dbName", true}, {"", false}})
}

func setMysqlCatalogTestFunc(t *testing.T, query string, expectedValues [][]any) {
	statements := utils.GetSeparateSqlStatements(query)
	dialect := &driver_infrastructure.MySQLDatabaseDialect{}
	assert.Equal(t, len(expectedValues), len(statements))

	for i, statement := range statements {
		catalog, ok := dialect.DoesStatementSetCatalog(statement)
		assert.Equal(t, expectedValues[i][0], catalog)
		assert.Equal(t, expectedValues[i][1], ok)
	}
}

func TestMysqlDoesSetTxIsolation(t *testing.T) {
	setMysqlTxIsolationTestFunc(t, " select 1 ", [][]any{{driver_infrastructure.TRANSACTION_READ_UNCOMMITTED, false}})
	setMysqlTxIsolationTestFunc(t, " select /* COMMENT */ 1 ", [][]any{{driver_infrastructure.TRANSACTION_READ_UNCOMMITTED, false}})
	setMysqlTxIsolationTestFunc(t, " /* COMMENT */ select /* COMMENT */ 1 ", [][]any{{driver_infrastructure.TRANSACTION_READ_UNCOMMITTED, false}})
	setMysqlTxIsolationTestFunc(t, " set session transaction isolation level read uncommitted ", [][]any{{driver_infrastructure.TRANSACTION_READ_UNCOMMITTED, true}})
	setMysqlTxIsolationTestFunc(t, " set session transaction isolation level read committed ", [][]any{{driver_infrastructure.TRANSACTION_READ_COMMITTED, true}})
	setMysqlTxIsolationTestFunc(t, " set session transaction isolation level repeatable read ", [][]any{{driver_infrastructure.TRANSACTION_REPEATABLE_READ, true}})
	setMysqlTxIsolationTestFunc(t, " set session transaction isolation level serializable ", [][]any{{driver_infrastructure.TRANSACTION_SERIALIZABLE, true}})
	setMysqlTxIsolationTestFunc(
		t,
		" set session transaction isolation level serializable ;"+
			" set session transaction isolation level repeatable read  ;",
		[][]any{
			{driver_infrastructure.TRANSACTION_SERIALIZABLE, true},
			{driver_infrastructure.TRANSACTION_REPEATABLE_READ, true},
		},
	)
	setMysqlTxIsolationTestFunc(
		t,
		" set session transaction /* COMMENT */isolation level read uncommitted ;"+
			"select 1;"+
			" set session transaction /* COMMENT */ isolation level read committed ",
		[][]any{
			{driver_infrastructure.TRANSACTION_READ_UNCOMMITTED, true},
			{driver_infrastructure.TRANSACTION_READ_UNCOMMITTED, false},
			{driver_infrastructure.TRANSACTION_READ_COMMITTED, true},
		},
	)
}

func setMysqlTxIsolationTestFunc(t *testing.T, query string, expectedValues [][]any) {
	statements := utils.GetSeparateSqlStatements(query)
	dialect := &driver_infrastructure.MySQLDatabaseDialect{}
	assert.Equal(t, len(expectedValues), len(statements))

	for i, statement := range statements {
		level, ok := dialect.DoesStatementSetTransactionIsolation(statement)
		assert.Equal(t, expectedValues[i][0], level)
		assert.Equal(t, expectedValues[i][1], ok)
	}
}

func TestMysqlDoesSetSchema(t *testing.T) {
	dialect := &driver_infrastructure.MySQLDatabaseDialect{}
	catalog, ok := dialect.DoesStatementSetCatalog("anything")
	assert.Empty(t, catalog)
	assert.False(t, ok)
}

func TestMysqlGetSetAutoCommitQuery(t *testing.T) {
	dialect := &driver_infrastructure.MySQLDatabaseDialect{}
	query, err := dialect.GetSetAutoCommitQuery(true)
	assert.Nil(t, err)
	assert.Equal(t, "set autocommit=true", query)
	query, err = dialect.GetSetAutoCommitQuery(false)
	assert.Nil(t, err)
	assert.Equal(t, "set autocommit=false", query)
}

func TestMysqlGetSetReadOnlyQuery(t *testing.T) {
	dialect := &driver_infrastructure.MySQLDatabaseDialect{}
	query, err := dialect.GetSetReadOnlyQuery(true)
	assert.Nil(t, err)
	assert.Equal(t, "set session transaction read only", query)
	query, err = dialect.GetSetReadOnlyQuery(false)
	assert.Nil(t, err)
	assert.Equal(t, "set session transaction read write", query)
}

func TestMysqlGetSetCatalogQuery(t *testing.T) {
	dialect := &driver_infrastructure.MySQLDatabaseDialect{}
	query, err := dialect.GetSetCatalogQuery("catalog")
	assert.Nil(t, err)
	assert.Equal(t, "use catalog", query)
}

func TestMysqlGetSetSchemaQuery(t *testing.T) {
	dialect := &driver_infrastructure.MySQLDatabaseDialect{}
	query, err := dialect.GetSetSchemaQuery("schema")
	assert.Empty(t, query)
	assert.Error(t, err)
}

func TestMysqlGetSetTransactionIsolationQuery(t *testing.T) {
	dialect := &driver_infrastructure.MySQLDatabaseDialect{}
	query, err := dialect.GetSetTransactionIsolationQuery(driver_infrastructure.TRANSACTION_READ_COMMITTED)
	assert.Nil(t, err)
	assert.Equal(t, "set session transaction isolation level READ COMMITTED", query)

	query, err = dialect.GetSetTransactionIsolationQuery(driver_infrastructure.TRANSACTION_READ_UNCOMMITTED)
	assert.Nil(t, err)
	assert.Equal(t, "set session transaction isolation level READ UNCOMMITTED", query)

	query, err = dialect.GetSetTransactionIsolationQuery(driver_infrastructure.TRANSACTION_REPEATABLE_READ)
	assert.Nil(t, err)
	assert.Equal(t, "set session transaction isolation level REPEATABLE READ", query)

	query, err = dialect.GetSetTransactionIsolationQuery(driver_infrastructure.TRANSACTION_SERIALIZABLE)
	assert.Nil(t, err)
	assert.Equal(t, "set session transaction isolation level SERIALIZABLE", query)
}

func TestAuroraMySQLDatabaseDialect_GetBlueGreenStatus(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraMySQLDatabaseDialect{}
	expectedQuery := "SELECT version, endpoint, port, role, status FROM mysql.rds_topology"
	assert.Equal(t, expectedQuery, testDatabaseDialect.GetBlueGreenStatusQuery())
}

func TestAuroraMySQLDatabaseDialect_GetBlueGreenStatus_QueryError(t *testing.T) {
	// This test verified query error handling which is now in BlueGreenStatusMonitor.
	// We just verify the query string is correct.
	testDatabaseDialect := &driver_infrastructure.AuroraMySQLDatabaseDialect{}
	assert.NotEmpty(t, testDatabaseDialect.GetBlueGreenStatusQuery())
}

func TestAuroraMySQLDatabaseDialect_GetBlueGreenStatus_NoQueryerContext(t *testing.T) {
	// This test verified no QueryerContext handling which is now in BlueGreenStatusMonitor.
	// We just verify the query string is correct.
	testDatabaseDialect := &driver_infrastructure.AuroraMySQLDatabaseDialect{}
	assert.NotEmpty(t, testDatabaseDialect.GetBlueGreenStatusQuery())
}

func TestAuroraMySQLDatabaseDialect_IsBlueGreenStatusAvailable(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraMySQLDatabaseDialect{}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockQueryer := mock_database_sql_driver.NewMockQueryerContext(ctrl)
	mockRows := mock_database_sql_driver.NewMockRows(ctrl)

	conn := struct {
		driver.Conn
		driver.QueryerContext
	}{
		Conn:           mockConn,
		QueryerContext: mockQueryer,
	}

	expectedQuery := "SELECT 1 AS tmp FROM information_schema.tables WHERE table_schema = 'mysql' AND table_name = 'rds_topology'"

	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedQuery, gomock.Nil()).
		Return(mockRows, nil)

	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = int64(1)
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	result := testDatabaseDialect.IsBlueGreenStatusAvailable(conn)
	assert.True(t, result)

	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedQuery, gomock.Nil()).
		Return(mockRows, nil)

	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrBadConn)
	mockRows.EXPECT().Close().Return(nil)

	result = testDatabaseDialect.IsBlueGreenStatusAvailable(conn)
	assert.False(t, result)
}

func TestRdsMySQLDatabaseDialect_GetBlueGreenStatus(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMySQLDatabaseDialect{}
	expectedQuery := "SELECT version, endpoint, port, role, status FROM mysql.rds_topology"
	assert.Equal(t, expectedQuery, testDatabaseDialect.GetBlueGreenStatusQuery())
}

func TestRdsMySQLDatabaseDialect_GetBlueGreenStatus_EmptyResults(t *testing.T) {
	// This test verified empty result handling which is now in BlueGreenStatusMonitor.
	// We just verify the query string is correct.
	testDatabaseDialect := &driver_infrastructure.RdsMySQLDatabaseDialect{}
	assert.NotEmpty(t, testDatabaseDialect.GetBlueGreenStatusQuery())
}

func TestRdsMySQLDatabaseDialect_IsBlueGreenStatusAvailable(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMySQLDatabaseDialect{}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockQueryer := mock_database_sql_driver.NewMockQueryerContext(ctrl)
	mockRows := mock_database_sql_driver.NewMockRows(ctrl)

	conn := struct {
		driver.Conn
		driver.QueryerContext
	}{
		Conn:           mockConn,
		QueryerContext: mockQueryer,
	}

	expectedQuery := "SELECT 1 AS tmp FROM information_schema.tables WHERE table_schema = 'mysql' AND table_name = 'rds_topology'"

	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedQuery, gomock.Nil()).
		Return(mockRows, nil)

	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = int64(1)
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	result := testDatabaseDialect.IsBlueGreenStatusAvailable(conn)
	assert.True(t, result)
}

func TestRdsMySQLDatabaseDialect_IsBlueGreenStatusAvailable_QueryError(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMySQLDatabaseDialect{}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockQueryer := mock_database_sql_driver.NewMockQueryerContext(ctrl)

	conn := struct {
		driver.Conn
		driver.QueryerContext
	}{
		Conn:           mockConn,
		QueryerContext: mockQueryer,
	}

	expectedQuery := "SELECT 1 AS tmp FROM information_schema.tables WHERE table_schema = 'mysql' AND table_name = 'rds_topology'"

	// Test query error - should return false
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedQuery, gomock.Nil()).
		Return(nil, fmt.Errorf("connection error"))

	result := testDatabaseDialect.IsBlueGreenStatusAvailable(conn)
	assert.False(t, result)
}

func TestGetBlueGreenStatus_InvalidRowData(t *testing.T) {
	// This test verified invalid row data handling which is now in BlueGreenStatusMonitor.getBlueGreenStatus().
	// We verify the query string is correct for AuroraMySQLDatabaseDialect.
	testDatabaseDialect := &driver_infrastructure.AuroraMySQLDatabaseDialect{}
	expectedQuery := "SELECT version, endpoint, port, role, status FROM mysql.rds_topology"
	assert.Equal(t, expectedQuery, testDatabaseDialect.GetBlueGreenStatusQuery())
}

func TestGetBlueGreenStatus_InsufficientColumns(t *testing.T) {
	// This test verified insufficient column handling which is now in BlueGreenStatusMonitor.getBlueGreenStatus().
	// We verify the query string is correct for RdsMySQLDatabaseDialect.
	testDatabaseDialect := &driver_infrastructure.RdsMySQLDatabaseDialect{}
	expectedQuery := "SELECT version, endpoint, port, role, status FROM mysql.rds_topology"
	assert.Equal(t, expectedQuery, testDatabaseDialect.GetBlueGreenStatusQuery())
}
