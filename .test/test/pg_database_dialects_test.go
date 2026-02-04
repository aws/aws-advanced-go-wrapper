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
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	mock_database_sql_driver "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/database_sql_driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_info"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

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
	expectedDefaultPort := 5432
	assert.Equal(t, testDatabaseDialect.GetDefaultPort(), expectedDefaultPort)
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

	mockRows.EXPECT().Columns().Return([]string{"dummy"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = 1
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	assert.True(t, testDatabaseDialect.IsDialect(conn), "Expected PgDatabaseDialect.IsDialect to be true")

	// IsDialect - false
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)

	mockRows.EXPECT().Columns().Return([]string{})
	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrBadConn)
	mockRows.EXPECT().Close().Return(nil)

	assert.False(t, testDatabaseDialect.IsDialect(conn), "Expected PgDatabaseDialect.IsDialect to be false")
}

func TestPgDatabaseDialect_GetHostListProvider(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.PgDatabaseDialect{}
	hostListProvider := testDatabaseDialect.GetHostListProvider(
		emptyProps,
		nil,
		nil)

	dsnHostListProvider, ok := hostListProvider.(*driver_infrastructure.DsnHostListProvider)
	assert.True(t, ok, "expected a DsnHostListProvider to be returned")
	assert.NotNil(t, dsnHostListProvider)
}

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
	expectedDefaultPort := 5432

	assert.Equal(t, testDatabaseDialect.GetDefaultPort(), expectedDefaultPort)
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

	// Call to GetFirstRowFromQuery within embedded structure PgDialect.IsDialect.
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedPgIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"something"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = 1
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	// Second call to GetFirstRowFromQuery.
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedRdsPgIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"true", "false"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = true
		dest[1] = false
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	assert.True(t, testDatabaseDialect.IsDialect(conn), "Expected RdsPgDatabaseDialect.IsDialect to be true")

	// IsDialect - false.

	// First call to GetFirstRowFromQuery, but NOT pg dialect
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedPgIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)

	mockRows.EXPECT().Columns().Return([]string{})
	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrBadConn)
	mockRows.EXPECT().Close().Return(nil)

	assert.False(t, testDatabaseDialect.IsDialect(conn), "Expected RdsPgDatabaseDialect.IsDialect to be false")

	// Call to GetFirstRowFromQuery within embedded structure PgDialect.IsDialect.
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedPgIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"something"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = 1
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	// Second call to query but it is aurora
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedRdsPgIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"rds_tools", "aurora_stat_util"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = true
		dest[1] = true
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	assert.False(t, testDatabaseDialect.IsDialect(conn), "Expected RdsPgDatabaseDialect.IsDialect to be true")
}

func TestRdsPgDatabaseDialect_GetHostListProvider(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsPgDatabaseDialect{}
	hostListProvider := testDatabaseDialect.GetHostListProvider(
		emptyProps,
		nil,
		nil)

	dsnHostListProvider, ok := hostListProvider.(*driver_infrastructure.DsnHostListProvider)
	assert.True(t, ok, "expected a DsnHostListProvider to be returned")
	assert.NotNil(t, dsnHostListProvider)
}

func TestAuroraRdsPgDatabaseDialect_GetDialectUpdateCandidates(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraPgDatabaseDialect{}
	expectedCandidates := []string{
		driver_infrastructure.RDS_PG_MULTI_AZ_CLUSTER_DIALECT,
	}

	assert.ElementsMatch(t, expectedCandidates, testDatabaseDialect.GetDialectUpdateCandidates())
}

func TestAuroraRdsPgDatabaseDialect_GetDefaultPort(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraPgDatabaseDialect{}
	expectedDefaultPort := 5432

	assert.Equal(t, testDatabaseDialect.GetDefaultPort(), expectedDefaultPort)
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

	// Call to GetFirstRowFromQuery within embedded structure PgDialect.IsDialect.
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedPgIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"something"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = 1
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	// Second call to GetFirstRowFromQuery.
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedRdsPgIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"true", "false"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = true
		dest[1] = false
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	// Third call to GetFirstRowFromQuery.
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedTopologyQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"1"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = 1
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	assert.True(t, testDatabaseDialect.IsDialect(conn), "Expected RdsPgDatabaseDialect.IsDialect to be true")

	// IsDialect - false.

	// First call to GetFirstRowFromQuery, but NOT pg dialect
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedPgIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)

	mockRows.EXPECT().Columns().Return([]string{})
	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrBadConn)
	mockRows.EXPECT().Close().Return(nil)

	assert.False(t, testDatabaseDialect.IsDialect(conn), "Expected RdsPgDatabaseDialect.IsDialect to be false")

	// Call to GetFirstRowFromQuery within embedded structure PgDialect.IsDialect.
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedPgIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"something"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = 1
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	// Second call to query but it is aurora
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedRdsPgIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"rds_tools"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = false // should cause IsDialect to be false
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	// Third call to GetFirstRowFromQuery.
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedTopologyQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"1"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = 1
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	assert.False(t, testDatabaseDialect.IsDialect(conn), "Expected RdsPgDatabaseDialect.IsDialect to be true")

	// First call to GetFirstRowFromQuery, but NOT pg dialect
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedPgIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)

	mockRows.EXPECT().Columns().Return([]string{})
	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrBadConn)
	mockRows.EXPECT().Close().Return(nil)

	assert.False(t, testDatabaseDialect.IsDialect(conn), "Expected RdsPgDatabaseDialect.IsDialect to be false")

	// Call to GetFirstRowFromQuery within embedded structure PgDialect.IsDialect.
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedPgIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"something"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = 1
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	// Second call to query but it is aurora
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedRdsPgIsDialectQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"rds_tools"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = true
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	// Third call to GetFirstRowFromQuery.
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedTopologyQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"1"})
	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrBadConn)
	mockRows.EXPECT().Close().Return(nil)

	assert.False(t, testDatabaseDialect.IsDialect(conn), "Expected RdsPgDatabaseDialect.IsDialect to be true")
}

func TestAuroraRdsPgDatabaseDialect_GetHostListProvider(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraPgDatabaseDialect{}

	propsNoFailover := emptyProps
	property_util.PLUGINS.Set(propsNoFailover, "efm")
	hostListProvider := testDatabaseDialect.GetHostListProvider(
		propsNoFailover,
		nil,
		nil)

	rdsHostListProvider, ok := hostListProvider.(*driver_infrastructure.RdsHostListProvider)
	assert.True(t, ok, "expected an RdsHostListProvider to be returned")
	assert.NotNil(t, rdsHostListProvider)

	propsWithFailover := emptyProps
	property_util.PLUGINS.Set(propsWithFailover, "failover")
	hostListProvider = testDatabaseDialect.GetHostListProvider(
		propsWithFailover,
		nil,
		nil)

	monitoringHostListProvider, ok := hostListProvider.(*driver_infrastructure.MonitoringRdsHostListProvider)
	assert.True(t, ok, "expected a MonitoringRdsHostListProvider to be returned")
	assert.NotNil(t, monitoringHostListProvider)
}

func TestAuroraRdsPgDatabaseDialect_GetHostRole(t *testing.T) {
	isReaderQuery := "SELECT pg_catalog.pg_is_in_recovery()"
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

	// writer
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), isReaderQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"true"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = false
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	assert.Equal(t, host_info_util.WRITER, testDatabaseDialect.GetHostRole(conn))

	// reader
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), isReaderQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"false"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = true
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	assert.Equal(t, host_info_util.READER, testDatabaseDialect.GetHostRole(conn))

	// unknown
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), isReaderQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"false"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = "unknown"
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	assert.Equal(t, host_info_util.UNKNOWN, testDatabaseDialect.GetHostRole(conn))
}

func TestAuroraRdsPgDatabaseDialect_GetTopology(t *testing.T) {
	topologyQuery := "SELECT server_id, CASE WHEN SESSION_ID OPERATOR(pg_catalog.=) 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END AS is_writer, " +
		"CPU, COALESCE(REPLICA_LAG_IN_MSEC, 0) AS lag, LAST_UPDATE_TIMESTAMP " +
		"FROM pg_catalog.aurora_replica_status() " +
		// Filter out hosts that haven't been updated in the last 5 minutes.
		"WHERE EXTRACT(EPOCH FROM(pg_catalog.NOW() OPERATOR(pg_catalog.-) LAST_UPDATE_TIMESTAMP)) OPERATOR(pg_catalog.<=) 300 OR SESSION_ID OPERATOR(pg_catalog.=) " +
		"'MASTER_SESSION_ID' OR LAST_UPDATE_TIMESTAMP IS NULL"
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Mocks
	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockQueryer := mock_database_sql_driver.NewMockQueryerContext(ctrl)
	mockRows := mock_database_sql_driver.NewMockRows(ctrl)
	mockProvider := mock_driver_infrastructure.NewMockHostListProvider(ctrl)

	// Expected values
	currentTime := time.Now()
	columnNames := []string{"server_id", "is_writer", "CPU", "lag", "LAST_UPDATE_TIMESTAMP"}
	rowData := []driver.Value{"host1", true, 2.5, 100.0, currentTime}
	rowData2 := []driver.Value{"host2", false, 3.5, 50.0, currentTime}

	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), topologyQuery, gomock.Any()).
		Return(mockRows, nil)

	mockRows.EXPECT().
		Columns().
		Return(columnNames)

	mockRows.EXPECT().
		Next(gomock.Any()).
		DoAndReturn(func(dest []driver.Value) error {
			copy(dest, rowData)
			return nil
		})
	mockRows.EXPECT().
		Next(gomock.Any()).
		DoAndReturn(func(dest []driver.Value) error {
			copy(dest, rowData2)
			return nil
		})
	mockRows.EXPECT().
		Next(gomock.Any()).
		Return(driver.ErrSkip)

	mockRows.EXPECT().Close().Return(nil)

	mockProvider.EXPECT().
		CreateHost("host1", host_info_util.WRITER, 100.0, 2.5, gomock.Any()).
		Return(&host_info_util.HostInfo{
			Host:           "host1",
			Role:           host_info_util.WRITER,
			Weight:         10,
			LastUpdateTime: currentTime,
		})

	mockProvider.EXPECT().
		CreateHost("host2", host_info_util.READER, 50.0, 3.5, gomock.Any()).
		Return(&host_info_util.HostInfo{
			Host:           "host2",
			Role:           host_info_util.READER,
			Weight:         20,
			LastUpdateTime: currentTime,
		})

	// Test the actual function
	testDatabaseDialect := &driver_infrastructure.AuroraPgDatabaseDialect{}

	conn := struct {
		driver.Conn
		driver.QueryerContext
	}{
		Conn:           mockConn,
		QueryerContext: mockQueryer,
	}
	hosts, err := testDatabaseDialect.GetTopology(conn, mockProvider)

	assert.NoError(t, err)
	assert.Len(t, hosts, 2)
	assert.Equal(t, "host1", hosts[0].Host)
	assert.Equal(t, host_info_util.WRITER, hosts[0].Role)
	assert.Equal(t, "host2", hosts[1].Host)
	assert.Equal(t, host_info_util.READER, hosts[1].Role)
}

func TestAuroraRdsPgDatabaseDialect_GetHostName(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraPgDatabaseDialect{}
	hostIdQuery := "SELECT pg_catalog.aurora_db_instance_identifier()"
	instanceId := "myinstance"
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

	// Success
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), hostIdQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{instanceId})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = instanceId
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	id, _ := testDatabaseDialect.GetHostName(conn)
	assert.Equal(t, instanceId, id)

	// No Success
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), hostIdQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	emptyId, _ := testDatabaseDialect.GetHostName(conn)
	assert.Equal(t, "", emptyId)
}

func TestAuroraRdsPgDatabaseDialect_GetWriterHostName(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraPgDatabaseDialect{}
	hostIdQuery := "SELECT server_id FROM pg_catalog.aurora_replica_status() " +
		"WHERE SESSION_ID OPERATOR(pg_catalog.=) 'MASTER_SESSION_ID' AND SERVER_ID OPERATOR(pg_catalog.=) pg_catalog.aurora_db_instance_identifier()"
	instanceId := "myinstance"
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

	// Success
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), hostIdQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{instanceId})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = instanceId
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	result, err := testDatabaseDialect.GetWriterHostName(conn)
	assert.NoError(t, err)
	assert.Equal(t, instanceId, result)

	// No Success
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), hostIdQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	result, err = testDatabaseDialect.GetWriterHostName(conn)
	assert.NoError(t, err)
	assert.Equal(t, "", result)
}

func TestAuroraRdsPgDatabaseDialect_GetLimitlessRouterEndpointQuery(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraPgDatabaseDialect{}
	expectedLimitlessQuery := "select router_endpoint, load from pg_catalog.aurora_limitless_router_endpoints()"
	assert.Equal(t, expectedLimitlessQuery, testDatabaseDialect.GetLimitlessRouterEndpointQuery())
}

func TestRdsMultiAzDbClusterPgDialect_GetDialectUpdateCandidates(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMultiAzClusterPgDatabaseDialect{}
	var expectedCandidates []string

	assert.ElementsMatch(t, expectedCandidates, testDatabaseDialect.GetDialectUpdateCandidates())
}

func TestRdsMultiAzDbClusterPgDialect_GetDefaultPort(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMultiAzClusterPgDatabaseDialect{}
	expectedDefaultPort := 5432

	assert.Equal(t, testDatabaseDialect.GetDefaultPort(), expectedDefaultPort)
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

func TestRdsMultiAzDbClusterPgDialect_GetHostListProvider(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMultiAzClusterPgDatabaseDialect{}

	propsNoFailover := emptyProps
	property_util.PLUGINS.Set(propsNoFailover, "efm")
	hostListProvider := testDatabaseDialect.GetHostListProvider(
		propsNoFailover,
		nil,
		nil)

	rdsHostListProvider, ok := hostListProvider.(*driver_infrastructure.RdsHostListProvider)
	assert.True(t, ok, "expected an RdsHostListProvider to be returned")
	assert.NotNil(t, rdsHostListProvider)

	propsWithFailover := emptyProps
	property_util.PLUGINS.Set(propsWithFailover, "failover")
	hostListProvider = testDatabaseDialect.GetHostListProvider(
		propsWithFailover,
		nil,
		nil)

	monitoringHostListProvider, ok := hostListProvider.(*driver_infrastructure.MonitoringRdsHostListProvider)
	assert.True(t, ok, "expected a MonitoringRdsHostListProvider to be returned")
	assert.NotNil(t, monitoringHostListProvider)
}

func TestRdsMultiAzDbClusterPgDialect_GetHostRole(t *testing.T) {
	isReaderQuery := "SELECT pg_catalog.pg_is_in_recovery()"
	testDatabaseDialect := &driver_infrastructure.RdsMultiAzClusterPgDatabaseDialect{}
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

	// writer
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), isReaderQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"true"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = false
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	assert.Equal(t, host_info_util.WRITER, testDatabaseDialect.GetHostRole(conn))

	// reader
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), isReaderQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"false"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = true
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	assert.Equal(t, host_info_util.READER, testDatabaseDialect.GetHostRole(conn))

	// unknown
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), isReaderQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"false"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = "unknown"
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	assert.Equal(t, host_info_util.UNKNOWN, testDatabaseDialect.GetHostRole(conn))
}

func TestRdsMultiAzDbClusterPgDialect_GetHostName(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMultiAzClusterPgDatabaseDialect{}
	hostIdQuery := "SELECT serverid, endpoint FROM rds_tools.db_instance_identifier()"
	instanceId := "myinstance"
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

	// Success
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), hostIdQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{instanceId})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = instanceId
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	id, _ := testDatabaseDialect.GetHostName(conn)
	assert.Equal(t, instanceId, id)

	// No Success
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), hostIdQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	emptyId, _ := testDatabaseDialect.GetHostName(conn)
	assert.Equal(t, "", emptyId)
}

func TestRdsMultiAzDbClusterPgDialect_GetWriterHostName(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMultiAzClusterPgDatabaseDialect{}
	hostIdQuery := "SELECT endpoint FROM rds_tools.show_topology('aws-advanced-go-wrapper') as topology " +
		"WHERE topology.id OPERATOR(pg_catalog.=) (SELECT multi_az_db_cluster_source_dbi_resource_id FROM rds_tools.multi_az_db_cluster_source_dbi_resource_id()) " +
		"AND topology.id OPERATOR(pg_catalog.=) (SELECT dbi_resource_id FROM rds_tools.dbi_resource_id())"

	instanceId := "myinstance"
	instanceEndpoint := instanceId + ".com"
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

	// Success
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), hostIdQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{instanceEndpoint})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = instanceEndpoint
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	result, err := testDatabaseDialect.GetWriterHostName(conn)
	assert.NoError(t, err)
	assert.Equal(t, instanceId, result)

	// No Success
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), hostIdQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	result, err = testDatabaseDialect.GetWriterHostName(conn)
	assert.NoError(t, err)
	assert.Equal(t, "", result)
}

func TestRdsMultiAzDbClusterPgDialect_GetTopology(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockQueryer := mock_database_sql_driver.NewMockQueryerContext(ctrl)
	mockProvider := mock_driver_infrastructure.NewMockHostListProvider(ctrl)
	mockTopologyRows := mock_database_sql_driver.NewMockRows(ctrl)
	mockHostIdRows := mock_database_sql_driver.NewMockRows(ctrl)

	dialect := &driver_infrastructure.RdsMultiAzClusterPgDatabaseDialect{}
	expectedWriterHostName := "writer-host"
	expectedReaderHostName := "reader-host"
	expectedWriterEndpoint := expectedWriterHostName + ".com"
	expectedReaderEndpoint := expectedReaderHostName + ".com"

	// Mock getting writer host id
	hostIdQuery := "SELECT multi_az_db_cluster_source_dbi_resource_id " +
		"FROM rds_tools.multi_az_db_cluster_source_dbi_resource_id()"

	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), hostIdQuery, gomock.Nil()).
		Return(mockHostIdRows, nil)

	mockHostIdRows.EXPECT().Columns().Return([]string{"dbi_resource_id"})
	mockHostIdRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = expectedWriterHostName
		return nil
	})
	mockHostIdRows.EXPECT().Close().Return(nil)

	// Mock Topology query
	version := driver_info.AWS_ADVANCED_GO_WRAPPER_VERSION
	topologyQuery := fmt.Sprintf("SELECT id, endpoint FROM rds_tools.show_topology('aws-advanced-go-wrapper-%v')", version)

	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), topologyQuery, gomock.Nil()).
		Return(mockTopologyRows, nil)

	mockTopologyRows.EXPECT().Columns().Return([]string{"id", "endpoint"})

	mockTopologyRows.EXPECT().
		Next(gomock.Any()).
		DoAndReturn(func(dest []driver.Value) error {
			dest[0] = expectedWriterHostName
			dest[1] = expectedWriterEndpoint
			return nil
		})
	mockTopologyRows.EXPECT().
		Next(gomock.Any()).
		DoAndReturn(func(dest []driver.Value) error {
			dest[0] = expectedReaderHostName
			dest[1] = expectedReaderEndpoint
			return nil
		})
	mockTopologyRows.EXPECT().
		Next(gomock.Any()).
		Return(driver.ErrSkip)

	mockTopologyRows.EXPECT().Close().Return(nil)

	mockProvider.EXPECT().
		CreateHost(expectedWriterHostName, host_info_util.WRITER, 0.0, 0.0, gomock.Any()).
		Return(&host_info_util.HostInfo{
			Host: expectedWriterHostName,
			Role: host_info_util.WRITER,
		})

	mockProvider.EXPECT().
		CreateHost(expectedReaderHostName, host_info_util.READER, 0.0, 0.0, gomock.Any()).
		Return(&host_info_util.HostInfo{
			Host: expectedReaderHostName,
			Role: host_info_util.READER,
		})

	conn := struct {
		driver.Conn
		driver.QueryerContext
	}{
		Conn:           mockConn,
		QueryerContext: mockQueryer,
	}

	// Call Get Topology
	hosts, err := dialect.GetTopology(conn, mockProvider)
	assert.NoError(t, err)
	assert.Len(t, hosts, 2)
	assert.Equal(t, expectedWriterHostName, hosts[0].Host)
	assert.Equal(t, host_info_util.WRITER, hosts[0].Role)
	assert.Equal(t, expectedReaderHostName, hosts[1].Host)
	assert.Equal(t, host_info_util.READER, hosts[1].Role)
}

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

func TestAuroraPgDatabaseDialect_GetBlueGreenStatus(t *testing.T) {
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

	expectedQuery := "SELECT version, endpoint, port, role, status FROM pg_catalog.get_blue_green_fast_switchover_metadata(" +
		"'aws_advanced_go_wrapper-" + driver_info.AWS_ADVANCED_GO_WRAPPER_VERSION + "')"

	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedQuery, gomock.Nil()).
		Return(mockRows, nil)

	mockRows.EXPECT().Columns().Return([]string{"id", "endpoint", "port", "role", "status", "version"})

	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[1] = "prod-aurora-cluster.cluster-abc123def456.us-east-1.rds.amazonaws.com" // endpoint
		dest[2] = int64(5432)                                                            // port
		dest[3] = "BLUE_GREEN_DEPLOYMENT_SOURCE"                                         // role
		dest[4] = "AVAILABLE"                                                            // status
		dest[0] = "1.0"                                                                  // version
		return nil
	})

	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[1] = "prod-aurora-cluster-target.cluster-xyz789def456.us-east-1.rds.amazonaws.com" // endpoint
		dest[2] = int64(5432)                                                                   // port
		dest[3] = "BLUE_GREEN_DEPLOYMENT_TARGET"                                                // role
		dest[4] = "SWITCHOVER_IN_PROGRESS"                                                      // status
		dest[0] = "1.1"                                                                         // version
		return nil
	})

	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrSkip)
	mockRows.EXPECT().Close().Return(nil)

	results := testDatabaseDialect.GetBlueGreenStatus(conn)

	assert.Len(t, results, 2)

	assert.Equal(t, "1.0", results[0].Version)
	assert.Equal(t, "prod-aurora-cluster.cluster-abc123def456.us-east-1.rds.amazonaws.com", results[0].Endpoint)
	assert.Equal(t, 5432, results[0].Port)
	assert.Equal(t, "BLUE_GREEN_DEPLOYMENT_SOURCE", results[0].Role)
	assert.Equal(t, "AVAILABLE", results[0].Status)

	assert.Equal(t, "1.1", results[1].Version)
	assert.Equal(t, "prod-aurora-cluster-target.cluster-xyz789def456.us-east-1.rds.amazonaws.com", results[1].Endpoint)
	assert.Equal(t, 5432, results[1].Port)
	assert.Equal(t, "BLUE_GREEN_DEPLOYMENT_TARGET", results[1].Role)
	assert.Equal(t, "SWITCHOVER_IN_PROGRESS", results[1].Status)
}

func TestAuroraPgDatabaseDialect_GetBlueGreenStatus_QueryError(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraPgDatabaseDialect{}
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

	expectedQuery := "SELECT version, endpoint, port, role, status FROM pg_catalog.get_blue_green_fast_switchover_metadata(" +
		"'aws_advanced_go_wrapper-" + driver_info.AWS_ADVANCED_GO_WRAPPER_VERSION + "')"

	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedQuery, gomock.Nil()).
		Return(nil, fmt.Errorf("function does not exist"))

	results := testDatabaseDialect.GetBlueGreenStatus(conn)
	assert.Nil(t, results)
}

func TestAuroraPgDatabaseDialect_GetBlueGreenStatus_NoQueryerContext(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraPgDatabaseDialect{}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)

	results := testDatabaseDialect.GetBlueGreenStatus(mockConn)
	assert.Nil(t, results)
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

	// Test when function exists (returns true)
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedQuery, gomock.Nil()).
		Return(mockRows, nil)

	mockRows.EXPECT().Columns().Return([]string{"regproc"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = "pg_catalog.get_blue_green_fast_switchover_metadata"
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	result := testDatabaseDialect.IsBlueGreenStatusAvailable(conn)
	assert.True(t, result)

	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedQuery, gomock.Nil()).
		Return(mockRows, nil)

	mockRows.EXPECT().Columns().Return([]string{})
	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrBadConn)
	mockRows.EXPECT().Close().Return(nil)

	result = testDatabaseDialect.IsBlueGreenStatusAvailable(conn)
	assert.False(t, result)
}

func TestRdsPgDatabaseDialect_GetBlueGreenStatus(t *testing.T) {
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

	expectedQuery := "SELECT version, endpoint, port, role, status FROM rds_tools.show_topology('aws_advanced_go_wrapper-" +
		driver_info.AWS_ADVANCED_GO_WRAPPER_VERSION + "')"

	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedQuery, gomock.Nil()).
		Return(mockRows, nil)

	mockRows.EXPECT().Columns().Return([]string{"id", "endpoint", "port", "role", "status", "version"})

	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = "2.0"
		dest[1] = "analytics-postgres.s1t2u3v4w5x6.us-west-2.rds.amazonaws.com"
		dest[2] = int64(5432)
		dest[3] = "BLUE_GREEN_DEPLOYMENT_SOURCE"
		dest[4] = "AVAILABLE"
		return nil
	})

	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrSkip)
	mockRows.EXPECT().Close().Return(nil)

	results := testDatabaseDialect.GetBlueGreenStatus(conn)

	assert.Len(t, results, 1)
	assert.Equal(t, "2.0", results[0].Version)
	assert.Equal(t, "analytics-postgres.s1t2u3v4w5x6.us-west-2.rds.amazonaws.com", results[0].Endpoint)
	assert.Equal(t, 5432, results[0].Port)
	assert.Equal(t, "BLUE_GREEN_DEPLOYMENT_SOURCE", results[0].Role)
	assert.Equal(t, "AVAILABLE", results[0].Status)
}

func TestRdsPgDatabaseDialect_GetBlueGreenStatus_EmptyResults(t *testing.T) {
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

	expectedQuery :=
		"SELECT version, endpoint, port, role, status FROM rds_tools.show_topology('aws_advanced_go_wrapper-" +
			driver_info.AWS_ADVANCED_GO_WRAPPER_VERSION + "')"

	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedQuery, gomock.Nil()).
		Return(mockRows, nil)

	mockRows.EXPECT().Columns().Return([]string{"id", "endpoint", "port", "role", "status", "version"})
	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrSkip) // No rows
	mockRows.EXPECT().Close().Return(nil)

	results := testDatabaseDialect.GetBlueGreenStatus(conn)
	assert.Empty(t, results)
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

	mockRows.EXPECT().Columns().Return([]string{"regproc"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = "rds_tools.show_topology"
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	result := testDatabaseDialect.IsBlueGreenStatusAvailable(conn)
	assert.True(t, result)

	// Test query error - should return false
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedQuery, gomock.Nil()).
		Return(nil, fmt.Errorf("connection error"))

	result = testDatabaseDialect.IsBlueGreenStatusAvailable(conn)
	assert.False(t, result)
}

func TestPgGetBlueGreenStatus_InvalidRowData(t *testing.T) {
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

	expectedQuery := "SELECT version, endpoint, port, role, status FROM pg_catalog.get_blue_green_fast_switchover_metadata(" +
		"'aws_advanced_go_wrapper-" + driver_info.AWS_ADVANCED_GO_WRAPPER_VERSION + "')"

	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedQuery, gomock.Nil()).
		Return(mockRows, nil)

	mockRows.EXPECT().Columns().Return([]string{"id", "endpoint", "port", "role", "status", "version"})

	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[1] = 12345
		dest[2] = int64(5432)
		dest[3] = "BLUE_GREEN_DEPLOYMENT_SOURCE"
		dest[4] = "AVAILABLE"
		dest[0] = "1.0"
		return nil
	})

	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[1] = "valid-endpoint.amazonaws.com"
		dest[2] = int64(5432)
		dest[3] = "BLUE_GREEN_DEPLOYMENT_TARGET"
		dest[4] = "AVAILABLE"
		dest[0] = "1.0"
		return nil
	})

	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrSkip)
	mockRows.EXPECT().Close().Return(nil)

	results := testDatabaseDialect.GetBlueGreenStatus(conn)

	assert.Len(t, results, 1)
	assert.Equal(t, "valid-endpoint.amazonaws.com", results[0].Endpoint)
}

func TestPgGetBlueGreenStatus_InsufficientColumns(t *testing.T) {
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

	expectedQuery := "SELECT version, endpoint, port, role, status FROM rds_tools.show_topology('aws_advanced_go_wrapper-" +
		driver_info.AWS_ADVANCED_GO_WRAPPER_VERSION + "')"

	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), expectedQuery, gomock.Nil()).
		Return(mockRows, nil)

	// Mock only 3 columns instead of required 6
	mockRows.EXPECT().Columns().Return([]string{"id", "endpoint", "port"})

	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = "1"
		dest[1] = "endpoint.amazonaws.com"
		dest[2] = int64(5432)
		return nil
	})

	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrSkip)
	mockRows.EXPECT().Close().Return(nil)

	results := testDatabaseDialect.GetBlueGreenStatus(conn)

	assert.Empty(t, results)
}
