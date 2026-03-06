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

	mock_database_sql_driver "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/database_sql_driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_info"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

// =============================================================================
// PgDatabaseDialect tests
// =============================================================================

func TestPgDatabaseDialect_GetDialectUpdateCandidates(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.PgDatabaseDialect{}
	expectedCandidates := []string{
		driver_infrastructure.RDS_PG_MULTI_AZ_CLUSTER_DIALECT,
		driver_infrastructure.AURORA_PG_DIALECT,
		driver_infrastructure.RDS_PG_DIALECT}
	assert.ElementsMatch(t, expectedCandidates, testDatabaseDialect.GetDialectUpdateCandidates())
}

func TestPgDatabaseDialect_GetDefaultPort(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.PgDatabaseDialect{}
	assert.Equal(t, 5432, testDatabaseDialect.GetDefaultPort())
}

func TestPgDatabaseDialect_GetHostAlias(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.PgDatabaseDialect{}
	expectedHostAliasQuery := "SELECT pg_catalog.CONCAT(pg_catalog.inet_server_addr(), ':', pg_catalog.inet_server_port())"
	assert.Equal(t, expectedHostAliasQuery, testDatabaseDialect.GetHostAliasQuery())
}

func TestPgDatabaseDialect_GetServerVersion(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.PgDatabaseDialect{}
	expectedGetServerVersionQuery := "SELECT 'version', pg_catalog.VERSION()"
	assert.Equal(t, expectedGetServerVersionQuery, testDatabaseDialect.GetServerVersionQuery())
}

func TestPgDatabaseDialect(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.PgDatabaseDialect{}
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

	expectedIsDialectQuery := "SELECT 1 FROM pg_catalog.pg_proc LIMIT 1"

	// IsDialect - true
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = 1
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)
	assert.True(t, testDatabaseDialect.IsDialect(conn))

	// IsDialect - false
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrBadConn)
	mockRows.EXPECT().Close().Return(nil)
	assert.False(t, testDatabaseDialect.IsDialect(conn))
}

func TestPgDatabaseDialect_GetHostListProviderSupplier(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.PgDatabaseDialect{}
	supplier := testDatabaseDialect.GetHostListProviderSupplier()
	assert.NotNil(t, supplier)
}

// =============================================================================
// RdsPgDatabaseDialect tests
// =============================================================================

func TestRdsPgDatabaseDialect_GetDialectUpdateCandidates(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsPgDatabaseDialect{}
	expectedCandidates := []string{
		driver_infrastructure.RDS_PG_MULTI_AZ_CLUSTER_DIALECT,
		driver_infrastructure.AURORA_PG_DIALECT,
		driver_infrastructure.RDS_PG_DIALECT}
	assert.ElementsMatch(t, expectedCandidates, testDatabaseDialect.GetDialectUpdateCandidates())
}

func TestRdsPgDatabaseDialect_GetDefaultPort(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsPgDatabaseDialect{}
	assert.Equal(t, 5432, testDatabaseDialect.GetDefaultPort())
}

func TestRdsPgDatabaseDialect_GetHostAliasQuery(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsPgDatabaseDialect{}
	expectedHostAliasQuery := "SELECT pg_catalog.CONCAT(pg_catalog.inet_server_addr(), ':', pg_catalog.inet_server_port())"
	assert.Equal(t, expectedHostAliasQuery, testDatabaseDialect.GetHostAliasQuery())
}

func TestRdsPgDatabaseDialect_GetServerVersion(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsPgDatabaseDialect{}
	expectedGetServerVersionQuery := "SELECT 'version', pg_catalog.VERSION()"
	assert.Equal(t, expectedGetServerVersionQuery, testDatabaseDialect.GetServerVersionQuery())
}

func TestRdsPgDatabaseDialect_IsDialect(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsPgDatabaseDialect{}
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
	expectedPgIsDialectQuery := "SELECT 1 FROM pg_catalog.pg_proc LIMIT 1"
	expectedRdsPgIsDialectQuery := "SELECT (setting LIKE '%rds_tools%') AS rds_tools, (setting LIKE '%aurora_stat_utils%') " +
		"AS aurora_stat_utils FROM pg_catalog.pg_settings WHERE name OPERATOR(pg_catalog.=) 'rds.extensions'"

	// IsDialect - true
	// PgDatabaseDialect.IsDialect -> CheckExistenceQueries (no Columns call)
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedPgIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = 1
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	// GetFirstRowFromQuery (calls Columns)
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedRdsPgIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"rds_tools", "aurora_stat_utils"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = true
		dest[1] = false
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)
	assert.True(t, testDatabaseDialect.IsDialect(conn))

	// IsDialect - false (not pg dialect)
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedPgIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrBadConn)
	mockRows.EXPECT().Close().Return(nil)
	assert.False(t, testDatabaseDialect.IsDialect(conn))

	// IsDialect - false (is aurora, not rds)
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedPgIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = 1
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedRdsPgIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"rds_tools", "aurora_stat_utils"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = true
		dest[1] = true
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)
	assert.False(t, testDatabaseDialect.IsDialect(conn))
}

func TestRdsPgDatabaseDialect_GetHostListProviderSupplier(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsPgDatabaseDialect{}
	supplier := testDatabaseDialect.GetHostListProviderSupplier()
	assert.NotNil(t, supplier)
}

// =============================================================================
// AuroraPgDatabaseDialect tests
// =============================================================================

func TestAuroraRdsPgDatabaseDialect_GetDialectUpdateCandidates(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraPgDatabaseDialect{}
	expectedCandidates := []string{
		driver_infrastructure.RDS_PG_MULTI_AZ_CLUSTER_DIALECT,
	}
	assert.ElementsMatch(t, expectedCandidates, testDatabaseDialect.GetDialectUpdateCandidates())
}

func TestAuroraRdsPgDatabaseDialect_GetDefaultPort(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraPgDatabaseDialect{}
	assert.Equal(t, 5432, testDatabaseDialect.GetDefaultPort())
}

func TestAuroraRdsPgDatabaseDialect_GetHostAliasQuery(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraPgDatabaseDialect{}
	expectedHostAliasQuery := "SELECT pg_catalog.CONCAT(pg_catalog.inet_server_addr(), ':', pg_catalog.inet_server_port())"
	assert.Equal(t, expectedHostAliasQuery, testDatabaseDialect.GetHostAliasQuery())
}

func TestAuroraRdsPgDatabaseDialect_GetServerVersion(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraPgDatabaseDialect{}
	expectedGetServerVersionQuery := "SELECT 'version', pg_catalog.VERSION()"
	assert.Equal(t, expectedGetServerVersionQuery, testDatabaseDialect.GetServerVersionQuery())
}

func TestAuroraRdsPgDatabaseDialect_IsDialect(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraPgDatabaseDialect{}
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
	expectedPgIsDialectQuery := "SELECT 1 FROM pg_catalog.pg_proc LIMIT 1"
	expectedRdsPgIsDialectQuery := "SELECT (setting LIKE '%aurora_stat_utils%') " +
		"AS aurora_stat_utils FROM pg_catalog.pg_settings WHERE name OPERATOR(pg_catalog.=) 'rds.extensions'"
	expectedTopologyQuery := "SELECT 1 FROM pg_catalog.aurora_replica_status() LIMIT 1"

	// IsDialect - true
	// PgDatabaseDialect.IsDialect -> CheckExistenceQueries
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedPgIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = 1
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	// GetFirstRowFromQuery for aurora_stat_utils
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedRdsPgIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"aurora_stat_utils"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = true
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	// GetFirstRowFromQuery for topology
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedTopologyQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"1"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = 1
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)
	assert.True(t, testDatabaseDialect.IsDialect(conn))

	// IsDialect - false (not pg dialect)
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedPgIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrBadConn)
	mockRows.EXPECT().Close().Return(nil)
	assert.False(t, testDatabaseDialect.IsDialect(conn))

	// IsDialect - false (aurora_stat_utils not present)
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedPgIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = 1
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedRdsPgIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"aurora_stat_utils"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = false
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	// Still checks topology even if aurora_stat_utils is false
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedTopologyQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"1"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = 1
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)
	assert.False(t, testDatabaseDialect.IsDialect(conn))

	// IsDialect - false (not pg dialect again)
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedPgIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrBadConn)
	mockRows.EXPECT().Close().Return(nil)
	assert.False(t, testDatabaseDialect.IsDialect(conn))

	// IsDialect - false (has aurora_stat_utils but no topology)
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedPgIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = 1
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedRdsPgIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"aurora_stat_utils"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = true
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedTopologyQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"1"})
	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrBadConn)
	mockRows.EXPECT().Close().Return(nil)
	assert.False(t, testDatabaseDialect.IsDialect(conn))
}

func TestAuroraRdsPgDatabaseDialect_GetHostListProviderSupplier(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraPgDatabaseDialect{}
	supplier := testDatabaseDialect.GetHostListProviderSupplier()
	assert.NotNil(t, supplier)
}

func TestAuroraRdsPgDatabaseDialect_GetTopologyQuery(t *testing.T) {
	dialect := &driver_infrastructure.AuroraPgDatabaseDialect{}
	expected := "SELECT server_id, CASE WHEN SESSION_ID OPERATOR(pg_catalog.=) 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END AS is_writer, " +
		"CPU, COALESCE(REPLICA_LAG_IN_MSEC, 0) AS lag, LAST_UPDATE_TIMESTAMP " +
		"FROM pg_catalog.aurora_replica_status() " +
		"WHERE EXTRACT(EPOCH FROM(pg_catalog.NOW() OPERATOR(pg_catalog.-) LAST_UPDATE_TIMESTAMP)) OPERATOR(pg_catalog.<=) 300 OR SESSION_ID OPERATOR(pg_catalog.=) " +
		"'MASTER_SESSION_ID' OR LAST_UPDATE_TIMESTAMP IS NULL"
	assert.Equal(t, expected, dialect.GetTopologyQuery())
}

func TestAuroraRdsPgDatabaseDialect_GetInstanceIdQuery(t *testing.T) {
	dialect := &driver_infrastructure.AuroraPgDatabaseDialect{}
	assert.Equal(t, "SELECT pg_catalog.aurora_db_instance_identifier()", dialect.GetInstanceIdQuery())
}

func TestAuroraRdsPgDatabaseDialect_GetWriterIdQuery(t *testing.T) {
	dialect := &driver_infrastructure.AuroraPgDatabaseDialect{}
	expected := "SELECT server_id FROM pg_catalog.aurora_replica_status() WHERE SESSION_ID OPERATOR(pg_catalog.=) 'MASTER_SESSION_ID' AND SERVER_ID OPERATOR(pg_catalog.=)" +
		" pg_catalog.aurora_db_instance_identifier()"
	assert.Equal(t, expected, dialect.GetWriterIdQuery())
}

func TestAuroraRdsPgDatabaseDialect_GetIsReaderQuery(t *testing.T) {
	dialect := &driver_infrastructure.AuroraPgDatabaseDialect{}
	assert.Equal(t, "SELECT pg_catalog.pg_is_in_recovery()", dialect.GetIsReaderQuery())
}

func TestAuroraRdsPgDatabaseDialect_GetLimitlessRouterEndpointQuery(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraPgDatabaseDialect{}
	expectedLimitlessQuery := "select router_endpoint, load from pg_catalog.aurora_limitless_router_endpoints()"
	assert.Equal(t, expectedLimitlessQuery, testDatabaseDialect.GetLimitlessRouterEndpointQuery())
}

// =============================================================================
// RdsMultiAzClusterPgDatabaseDialect tests
// =============================================================================

func TestRdsMultiAzDbClusterPgDialect_GetDialectUpdateCandidates(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMultiAzClusterPgDatabaseDialect{}
	var expectedCandidates []string
	assert.ElementsMatch(t, expectedCandidates, testDatabaseDialect.GetDialectUpdateCandidates())
}

func TestRdsMultiAzDbClusterPgDialect_GetDefaultPort(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMultiAzClusterPgDatabaseDialect{}
	assert.Equal(t, 5432, testDatabaseDialect.GetDefaultPort())
}

func TestRdsMultiAzDbClusterPgDialect_GetHostAliasQuery(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMultiAzClusterPgDatabaseDialect{}
	expectedHostAliasQuery := "SELECT pg_catalog.CONCAT(pg_catalog.inet_server_addr(), ':', pg_catalog.inet_server_port())"
	assert.Equal(t, expectedHostAliasQuery, testDatabaseDialect.GetHostAliasQuery())
}

func TestRdsMultiAzDbClusterPgDialect_GetServerVersion(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMultiAzClusterPgDatabaseDialect{}
	expectedGetServerVersionQuery := "SELECT 'version', pg_catalog.VERSION()"
	assert.Equal(t, expectedGetServerVersionQuery, testDatabaseDialect.GetServerVersionQuery())
}

func TestRdsMultiAzDbClusterPgDialect_GetHostListProviderSupplier(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMultiAzClusterPgDatabaseDialect{}
	supplier := testDatabaseDialect.GetHostListProviderSupplier()
	assert.NotNil(t, supplier)
}

func TestRdsMultiAzDbClusterPgDialect_GetTopologyQuery(t *testing.T) {
	dialect := &driver_infrastructure.RdsMultiAzClusterPgDatabaseDialect{}
	version := driver_info.AWS_ADVANCED_GO_WRAPPER_VERSION
	expected := fmt.Sprintf("SELECT id, endpoint FROM rds_tools.show_topology('aws-advanced-go-wrapper-%v')", version)
	assert.Equal(t, expected, dialect.GetTopologyQuery())
}

func TestRdsMultiAzDbClusterPgDialect_GetInstanceIdQuery(t *testing.T) {
	dialect := &driver_infrastructure.RdsMultiAzClusterPgDatabaseDialect{}
	expected := "SELECT id, SUBSTRING(endpoint FROM 0 FOR POSITION('.' IN endpoint))" +
		" FROM rds_tools.show_topology()" +
		" WHERE id OPERATOR(pg_catalog.=) rds_tools.dbi_resource_id()"
	assert.Equal(t, expected, dialect.GetInstanceIdQuery())
}

func TestRdsMultiAzDbClusterPgDialect_GetWriterIdQuery(t *testing.T) {
	dialect := &driver_infrastructure.RdsMultiAzClusterPgDatabaseDialect{}
	expected := "SELECT multi_az_db_cluster_source_dbi_resource_id" +
		" FROM rds_tools.multi_az_db_cluster_source_dbi_resource_id()" +
		" WHERE multi_az_db_cluster_source_dbi_resource_id OPERATOR(pg_catalog.!=)" +
		" (SELECT dbi_resource_id FROM rds_tools.dbi_resource_id())"
	assert.Equal(t, expected, dialect.GetWriterIdQuery())
}

func TestRdsMultiAzDbClusterPgDialect_GetWriterIdColumnName(t *testing.T) {
	dialect := &driver_infrastructure.RdsMultiAzClusterPgDatabaseDialect{}
	assert.Equal(t, "multi_az_db_cluster_source_dbi_resource_id", dialect.GetWriterIdColumnName())
}

func TestRdsMultiAzDbClusterPgDialect_GetIsReaderQuery(t *testing.T) {
	dialect := &driver_infrastructure.RdsMultiAzClusterPgDatabaseDialect{}
	assert.Equal(t, "SELECT pg_catalog.pg_is_in_recovery()", dialect.GetIsReaderQuery())
}

// =============================================================================
// PG DoesStatementSet* tests
// =============================================================================

func TestPgDoesSetReadOnly(t *testing.T) {
	setPgReadOnlyTestFunc(t, " select 1 ", [][]bool{{false, false}})
	setPgReadOnlyTestFunc(t, " select /* COMMENT */ 1 ", [][]bool{{false, false}})
	setPgReadOnlyTestFunc(t, " /* COMMENT */ select /* COMMENT */ 1 ", [][]bool{{false, false}})
	setPgReadOnlyTestFunc(t, " SET SESSION characteristics as transaction READ ONLY", [][]bool{{true, true}})
	setPgReadOnlyTestFunc(t, " set session characteristics as transaction read /* COMMENT */ only ", [][]bool{{true, true}})
	setPgReadOnlyTestFunc(t, " /* COMMENT */ set session characteristics as transaction read /* COMMENT */ only ", [][]bool{{true, true}})
	setPgReadOnlyTestFunc(t, " set session characteristics as transaction read write ", [][]bool{{false, true}})
	setPgReadOnlyTestFunc(t, " /* COMMENT */ set session characteristics as transaction /* COMMENT */ read write ", [][]bool{{false, true}})
	setPgReadOnlyTestFunc(t, " set session characteristics as transaction /* COMMENT */ read write ", [][]bool{{false, true}})
	setPgReadOnlyTestFunc(
		t,
		" set session characteristics as transaction read only; "+
			"set session characteristics as transaction read write",
		[][]bool{
			{true, true},
			{false, true},
		},
	)
	setPgReadOnlyTestFunc(t, " set session characteristics as transaction read only; select 1", [][]bool{{true, true}, {false, false}})
	setPgReadOnlyTestFunc(
		t,
		" set session characteristics as  /* COMMENT */transaction read only/* COMMENT */;"+
			" select 1",
		[][]bool{
			{true, true},
			{false, false},
		},
	)
	setPgReadOnlyTestFunc(t, " select 1; set session characteristics as transaction read only; ", [][]bool{{false, false}, {true, true}})
	setPgReadOnlyTestFunc(
		t,
		" set session characteristics as transaction read write;"+
			" set session characteristics as transaction read only; ",
		[][]bool{
			{false, true},
			{true, true},
		},
	)
	setPgReadOnlyTestFunc(
		t,
		" set session characteristics as transaction read only;"+
			" set session characteristics as TRANSACTION READ WRITE; ",
		[][]bool{
			{true, true},
			{false, true},
		},
	)
	setPgReadOnlyTestFunc(t, " select 1; set session characteristics as transaction read only; ", [][]bool{{false, false}, {true, true}})
	setPgReadOnlyTestFunc(t, " set session characteristics as transaction read write; select 1", [][]bool{{false, true}, {false, false}})
	setPgReadOnlyTestFunc(t, " select 1; set session characteristics as transaction read write; select 1", [][]bool{{false, false}, {false, true}, {false, false}})
}

func setPgReadOnlyTestFunc(t *testing.T, query string, expectedValues [][]bool) {
	statements := utils.GetSeparateSqlStatements(query)
	dialect := &driver_infrastructure.PgDatabaseDialect{}
	assert.Equal(t, len(expectedValues), len(statements))

	for i, statement := range statements {
		readOnly, ok := dialect.DoesStatementSetReadOnly(statement)
		assert.Equal(t, expectedValues[i][0], readOnly)
		assert.Equal(t, expectedValues[i][1], ok)
	}
}

func TestPgDoesSetSchema(t *testing.T) {
	setPgSchemaTestFunc(t, " select 1 ", [][]any{{"", false}})
	setPgSchemaTestFunc(t, " SELECT /* COMMENT */ 1 ", [][]any{{"", false}})
	setPgSchemaTestFunc(t, " /* COMMENT */ select /* COMMENT */ 1 ", [][]any{{"", false}})
	setPgSchemaTestFunc(t, " SET search_path to path ", [][]any{{"path", true}})
	setPgSchemaTestFunc(t, " set search_path/* COMMENT */ to path ", [][]any{{"path", true}})
	setPgSchemaTestFunc(t, " set search_path to path1 ; set search_path to path2 ", [][]any{{"path1", true}, {"path2", true}})
	setPgSchemaTestFunc(t, " set search_path to \"path1\" ; ", [][]any{{"path1", true}})
	setPgSchemaTestFunc(t, " set search_path to \"path 1\" ;", [][]any{{"path 1", true}})
	setPgSchemaTestFunc(t, " set search_path to path1, path2 ;  ", [][]any{{"path1, path2", true}})
	setPgSchemaTestFunc(t, " set search_path = path1 ;  ", [][]any{{"path1", true}})
	setPgSchemaTestFunc(t, " set search_path = PATH ;  ", [][]any{{"PATH", true}})
	setPgSchemaTestFunc(t, " set search_path to PATH ;  ", [][]any{{"PATH", true}})
	setPgSchemaTestFunc(t, " set search_path to pathToLocation ;  ", [][]any{{"pathToLocation", true}})
	setPgSchemaTestFunc(t, " set search_path TO pathToLocation ;  ", [][]any{{"pathToLocation", true}})
	setPgSchemaTestFunc(t, " set search_path = pathToLocation ;  ", [][]any{{"pathToLocation", true}})
	setPgSchemaTestFunc(t, " set search_path =\"pathToLocation\" ;  ", [][]any{{"pathToLocation", true}})
	setPgSchemaTestFunc(t, " set search_path=\"pathToLocation\" ;  ", [][]any{{"pathToLocation", true}})
	setPgSchemaTestFunc(t, " set search_path= \"path to Location\" ;  ", [][]any{{"path to Location", true}})
}

func setPgSchemaTestFunc(t *testing.T, query string, expectedValues [][]any) {
	statements := utils.GetSeparateSqlStatements(query)
	dialect := &driver_infrastructure.PgDatabaseDialect{}
	assert.Equal(t, len(expectedValues), len(statements))

	for i, statement := range statements {
		schema, ok := dialect.DoesStatementSetSchema(statement)
		assert.Equal(t, expectedValues[i][0], schema)
		assert.Equal(t, expectedValues[i][1], ok)
	}
}

func TestPgDoesSetTxIsolation(t *testing.T) {
	setPgTxIsolationTestFunc(t, " select 1 ", [][]any{{driver_infrastructure.TRANSACTION_READ_UNCOMMITTED, false}})
	setPgTxIsolationTestFunc(t, " select /* COMMENT */ 1 ", [][]any{{driver_infrastructure.TRANSACTION_READ_UNCOMMITTED, false}})
	setPgTxIsolationTestFunc(t, " /* COMMENT */ select /* COMMENT */ 1 ", [][]any{{driver_infrastructure.TRANSACTION_READ_UNCOMMITTED, false}})
	setPgTxIsolationTestFunc(
		t,
		" set session characteristics as transaction isolation level read uncommitted ",
		[][]any{{driver_infrastructure.TRANSACTION_READ_UNCOMMITTED, true}},
	)
	setPgTxIsolationTestFunc(
		t,
		" SET SESSION characteristics as transaction isolation level read committed ",
		[][]any{{driver_infrastructure.TRANSACTION_READ_COMMITTED, true}},
	)
	setPgTxIsolationTestFunc(
		t,
		" set session characteristics as transaction isolation level repeatable read ",
		[][]any{{driver_infrastructure.TRANSACTION_REPEATABLE_READ, true}},
	)
	setPgTxIsolationTestFunc(
		t,
		" set session characteristics as transaction isolation level serializable ",
		[][]any{{driver_infrastructure.TRANSACTION_SERIALIZABLE, true}},
	)
	setPgTxIsolationTestFunc(
		t,
		" set session characteristics as transaction isolation level serializable; "+
			" set session characteristics as transaction isolation level repeatable read ",
		[][]any{{driver_infrastructure.TRANSACTION_SERIALIZABLE, true}, {driver_infrastructure.TRANSACTION_REPEATABLE_READ, true}},
	)
	setPgTxIsolationTestFunc(
		t,
		" set session characteristics AS TRANSACTION /* COMMENT */isolation level read uncommitted; "+
			"select 1;"+
			" set session characteristics as transaction /* COMMENT */ isolation level READ COMMITTED; ",
		[][]any{
			{driver_infrastructure.TRANSACTION_READ_UNCOMMITTED, true},
			{driver_infrastructure.TRANSACTION_READ_UNCOMMITTED, false},
			{driver_infrastructure.TRANSACTION_READ_COMMITTED, true},
		},
	)
}

func setPgTxIsolationTestFunc(t *testing.T, query string, expectedValues [][]any) {
	statements := utils.GetSeparateSqlStatements(query)
	dialect := &driver_infrastructure.PgDatabaseDialect{}
	assert.Equal(t, len(expectedValues), len(statements))

	for i, statement := range statements {
		level, ok := dialect.DoesStatementSetTransactionIsolation(statement)
		assert.Equal(t, expectedValues[i][0], level)
		assert.Equal(t, expectedValues[i][1], ok)
	}
}

func TestPgDoesSetAutoCommit(t *testing.T) {
	dialect := &driver_infrastructure.PgDatabaseDialect{}
	autoCommit, ok := dialect.DoesStatementSetAutoCommit("anything")
	assert.False(t, autoCommit)
	assert.False(t, ok)
}

func TestPgDoesSetCatalog(t *testing.T) {
	dialect := &driver_infrastructure.PgDatabaseDialect{}
	catalog, ok := dialect.DoesStatementSetCatalog("anything")
	assert.Empty(t, catalog)
	assert.False(t, ok)
}

// =============================================================================
// PG GetSet*Query tests
// =============================================================================

func TestPgGetSetAutoCommitQuery(t *testing.T) {
	dialect := &driver_infrastructure.PgDatabaseDialect{}
	query, err := dialect.GetSetAutoCommitQuery(true)
	assert.Empty(t, query)
	assert.Error(t, err)
	query, err = dialect.GetSetAutoCommitQuery(false)
	assert.Empty(t, query)
	assert.Error(t, err)
}

func TestPgGetSetReadOnlyQuery(t *testing.T) {
	dialect := &driver_infrastructure.PgDatabaseDialect{}
	query, err := dialect.GetSetReadOnlyQuery(true)
	assert.Nil(t, err)
	assert.Equal(t, "set session characteristics as transaction read only", query)
	query, err = dialect.GetSetReadOnlyQuery(false)
	assert.Nil(t, err)
	assert.Equal(t, "set session characteristics as transaction read write", query)
}

func TestPgGetSetCatalogQuery(t *testing.T) {
	dialect := &driver_infrastructure.PgDatabaseDialect{}
	query, err := dialect.GetSetCatalogQuery("catalog")
	assert.Empty(t, query)
	assert.Error(t, err)
}

func TestPgGetSetSchemaQuery(t *testing.T) {
	dialect := &driver_infrastructure.PgDatabaseDialect{}

	query, err := dialect.GetSetSchemaQuery("path")
	assert.Nil(t, err)
	assert.Equal(t, "set search_path to path", query)

	query, err = dialect.GetSetSchemaQuery("path1 path2")
	assert.Nil(t, err)
	assert.Equal(t, "set search_path to \"path1 path2\"", query)

	query, err = dialect.GetSetSchemaQuery("\"path1 path2\"")
	assert.Nil(t, err)
	assert.Equal(t, "set search_path to \"path1 path2\"", query)

	query, err = dialect.GetSetSchemaQuery("\"path1\"")
	assert.Nil(t, err)
	assert.Equal(t, "set search_path to \"path1\"", query)
}

func TestPgGetSetTransactionIsolationQuery(t *testing.T) {
	dialect := &driver_infrastructure.PgDatabaseDialect{}
	query, err := dialect.GetSetTransactionIsolationQuery(driver_infrastructure.TRANSACTION_READ_COMMITTED)
	assert.Nil(t, err)
	assert.Equal(t, "set session characteristics as transaction isolation level READ COMMITTED", query)

	query, err = dialect.GetSetTransactionIsolationQuery(driver_infrastructure.TRANSACTION_READ_UNCOMMITTED)
	assert.Nil(t, err)
	assert.Equal(t, "set session characteristics as transaction isolation level READ UNCOMMITTED", query)

	query, err = dialect.GetSetTransactionIsolationQuery(driver_infrastructure.TRANSACTION_REPEATABLE_READ)
	assert.Nil(t, err)
	assert.Equal(t, "set session characteristics as transaction isolation level REPEATABLE READ", query)

	query, err = dialect.GetSetTransactionIsolationQuery(driver_infrastructure.TRANSACTION_SERIALIZABLE)
	assert.Nil(t, err)
	assert.Equal(t, "set session characteristics as transaction isolation level SERIALIZABLE", query)
}

// =============================================================================
// Blue/Green Status tests
// =============================================================================

func TestAuroraPgDatabaseDialect_GetBlueGreenStatusQuery(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraPgDatabaseDialect{}
	expectedQuery := "SELECT version, endpoint, port, role, status FROM pg_catalog.get_blue_green_fast_switchover_metadata(" +
		"'aws_advanced_go_wrapper-" + driver_info.AWS_ADVANCED_GO_WRAPPER_VERSION + "')"
	assert.Equal(t, expectedQuery, testDatabaseDialect.GetBlueGreenStatusQuery())
}

func TestAuroraPgDatabaseDialect_GetBlueGreenStatusQuery_QueryError(t *testing.T) {
	// This test verified query error handling which is now in BlueGreenStatusMonitor.
	// We just verify the query string is correct.
	testDatabaseDialect := &driver_infrastructure.AuroraPgDatabaseDialect{}
	assert.NotEmpty(t, testDatabaseDialect.GetBlueGreenStatusQuery())
}

func TestAuroraPgDatabaseDialect_GetBlueGreenStatusQuery_NoQueryerContext(t *testing.T) {
	// This test verified no QueryerContext handling which is now in BlueGreenStatusMonitor.
	// We just verify the query string is correct.
	testDatabaseDialect := &driver_infrastructure.AuroraPgDatabaseDialect{}
	assert.NotEmpty(t, testDatabaseDialect.GetBlueGreenStatusQuery())
}

func TestAuroraPgDatabaseDialect_IsBlueGreenStatusAvailable(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraPgDatabaseDialect{}
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

	expectedQuery := "SELECT 'pg_catalog.get_blue_green_fast_switchover_metadata'::regproc"

	// Available
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = "pg_catalog.get_blue_green_fast_switchover_metadata"
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)
	assert.True(t, testDatabaseDialect.IsBlueGreenStatusAvailable(conn))

	// Not available
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrBadConn)
	mockRows.EXPECT().Close().Return(nil)
	assert.False(t, testDatabaseDialect.IsBlueGreenStatusAvailable(conn))
}

func TestRdsPgDatabaseDialect_GetBlueGreenStatusQuery(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsPgDatabaseDialect{}
	expectedQuery := "SELECT version, endpoint, port, role, status FROM rds_tools.show_topology('aws_advanced_go_wrapper-" +
		driver_info.AWS_ADVANCED_GO_WRAPPER_VERSION + "')"
	assert.Equal(t, expectedQuery, testDatabaseDialect.GetBlueGreenStatusQuery())
}

func TestRdsPgDatabaseDialect_GetBlueGreenStatusQuery_EmptyResults(t *testing.T) {
	// This test verified empty result handling which is now in BlueGreenStatusMonitor.
	// We just verify the query string is correct.
	testDatabaseDialect := &driver_infrastructure.RdsPgDatabaseDialect{}
	assert.NotEmpty(t, testDatabaseDialect.GetBlueGreenStatusQuery())
}

func TestRdsPgDatabaseDialect_IsBlueGreenStatusAvailable(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsPgDatabaseDialect{}
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

	expectedQuery := "SELECT 'rds_tools.show_topology'::regproc"

	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = "rds_tools.show_topology"
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	result := testDatabaseDialect.IsBlueGreenStatusAvailable(conn)
	assert.True(t, result)
}

func TestRdsPgDatabaseDialect_IsBlueGreenStatusAvailable_QueryError(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsPgDatabaseDialect{}
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

	expectedQuery := "SELECT 'rds_tools.show_topology'::regproc"

	// Test query error - should return false
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedQuery, gomock.Nil()).
		Return(nil, fmt.Errorf("connection error"))

	result := testDatabaseDialect.IsBlueGreenStatusAvailable(conn)
	assert.False(t, result)
}

func TestGetBlueGreenStatus_InvalidRowData_Pg(t *testing.T) {
	// This test verified invalid row data handling which is now in BlueGreenStatusMonitor.getBlueGreenStatus().
	// We verify the query string is correct for AuroraPgDatabaseDialect.
	testDatabaseDialect := &driver_infrastructure.AuroraPgDatabaseDialect{}
	expectedQuery := "SELECT version, endpoint, port, role, status FROM pg_catalog.get_blue_green_fast_switchover_metadata(" +
		"'aws_advanced_go_wrapper-" + driver_info.AWS_ADVANCED_GO_WRAPPER_VERSION + "')"
	assert.Equal(t, expectedQuery, testDatabaseDialect.GetBlueGreenStatusQuery())
}

func TestGetBlueGreenStatus_InsufficientColumns_Pg(t *testing.T) {
	// This test verified insufficient column handling which is now in BlueGreenStatusMonitor.getBlueGreenStatus().
	// We verify the query string is correct for RdsPgDatabaseDialect.
	testDatabaseDialect := &driver_infrastructure.RdsPgDatabaseDialect{}
	expectedQuery := "SELECT version, endpoint, port, role, status FROM rds_tools.show_topology('aws_advanced_go_wrapper-" +
		driver_info.AWS_ADVANCED_GO_WRAPPER_VERSION + "')"
	assert.Equal(t, expectedQuery, testDatabaseDialect.GetBlueGreenStatusQuery())
}
