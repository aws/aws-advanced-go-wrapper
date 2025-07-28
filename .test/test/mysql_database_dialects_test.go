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
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"strconv"
	"testing"
	"time"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	mock_database_sql_driver "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/database_sql_driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestMySQLDatabaseDialect_GetDialectUpdateCandidates(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.MySQLDatabaseDialect{}

	expectedCandidates := []string{
		driver_infrastructure.RDS_MYSQL_MULTI_AZ_CLUSTER_DIALECT,
		driver_infrastructure.AURORA_MYSQL_DIALECT,
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
	hostListProvider := testDatabaseDialect.GetHostListProvider(
		make(map[string]string),
		"dsn",
		nil,
		nil)

	dsnHostListProvider, ok := hostListProvider.(*driver_infrastructure.DsnHostListProvider)
	assert.True(t, ok, "expected a DsnHostListProvider to be returned")
	assert.NotNil(t, dsnHostListProvider)
}

func TestRdsMySQLDatabaseDialect_GetDialectUpdateCandidates(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMySQLDatabaseDialect{}
	expectedCandidates := []string{
		driver_infrastructure.RDS_MYSQL_MULTI_AZ_CLUSTER_DIALECT,
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
	hostListProvider := testDatabaseDialect.GetHostListProvider(
		make(map[string]string),
		"dsn",
		nil,
		nil)

	dsnHostListProvider, ok := hostListProvider.(*driver_infrastructure.DsnHostListProvider)
	assert.True(t, ok, "expected a DsnHostListProvider to be returned")
	assert.NotNil(t, dsnHostListProvider)
}

func TestAuroraRdsMySQLDatabaseDialect_GetDialectUpdateCandidates(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraMySQLDatabaseDialect{}
	expectedCandidates := []string{
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

	propsNoFailover := make(map[string]string)
	property_util.PLUGINS.Set(propsNoFailover, "efm")
	hostListProvider := testDatabaseDialect.GetHostListProvider(
		propsNoFailover,
		"dsn",
		nil,
		nil)

	rdsHostListProvider, ok := hostListProvider.(*driver_infrastructure.RdsHostListProvider)
	assert.True(t, ok, "expected an RdsHostListProvider to be returned")
	assert.NotNil(t, rdsHostListProvider)

	propsWithFailover := make(map[string]string)
	property_util.PLUGINS.Set(propsWithFailover, "failover")
	hostListProvider = testDatabaseDialect.GetHostListProvider(
		propsWithFailover,
		"dsn",
		nil,
		nil)

	monitoringHostListProvider, ok := hostListProvider.(*driver_infrastructure.MonitoringRdsHostListProvider)
	assert.True(t, ok, "expected a MonitoringRdsHostListProvider to be returned")
	assert.NotNil(t, monitoringHostListProvider)
}

func TestAuroraRdsMySQLDatabaseDialect_GetHostRole(t *testing.T) {
	isReaderQuery := "SELECT @@innodb_read_only"
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

	// writer
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), isReaderQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"true"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = int64(0)
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
		dest[0] = int64(1)
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

func TestAuroraRdsMySQLDatabaseDialect_GetTopology(t *testing.T) {
	topologyQuery := "SELECT server_id, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END as is_writer, " +
		"cpu, REPLICA_LAG_IN_MILLISECONDS as 'lag', LAST_UPDATE_TIMESTAMP as last_update_timestamp " +
		"FROM information_schema.replica_host_status " +
		"WHERE time_to_sec(timediff(now(), LAST_UPDATE_TIMESTAMP)) <= 300 OR SESSION_ID = 'MASTER_SESSION_ID' "
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
	rowData := []driver.Value{[]uint8("host1"), int64(1), 2.5, 100.0, []uint8("time")}
	rowData2 := []driver.Value{[]uint8("host2"), int64(0), 3.5, 50.0, []uint8("time")}

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
	testDatabaseDialect := &driver_infrastructure.AuroraMySQLDatabaseDialect{}

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

func TestAuroraRdsMySQLDatabaseDialect_GetHostName(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraMySQLDatabaseDialect{}
	hostIdQuery := "SELECT @@aurora_server_id"
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
		dest[0] = []uint8(instanceId)
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	assert.Equal(t, instanceId, testDatabaseDialect.GetHostName(conn))

	// No Success
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), hostIdQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	assert.Equal(t, "", testDatabaseDialect.GetHostName(conn))
}

func TestAuroraRdsMySQLDatabaseDialect_GetWriterHostName(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.AuroraMySQLDatabaseDialect{}
	hostIdQuery := "SELECT server_id " +
		"FROM information_schema.replica_host_status " +
		"WHERE SESSION_ID = 'MASTER_SESSION_ID' AND SERVER_ID = @@aurora_server_id"
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

func TestRdsMultiAzDbClusterMySQLDialect_GetDialectUpdateCandidates(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMultiAzDbClusterMySQLDialect{}
	expectedCandidates := []string{}

	assert.ElementsMatch(t, expectedCandidates, testDatabaseDialect.GetDialectUpdateCandidates())
}

func TestRdsMultiAzDbClusterMySQLDialect_GetDefaultPort(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMultiAzDbClusterMySQLDialect{}
	expectedDefaultPort := 3306

	assert.Equal(t, testDatabaseDialect.GetDefaultPort(), expectedDefaultPort)
}

func TestRdsMultiAzDbClusterMySQLDialect_GetHostAliasQuery(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMultiAzDbClusterMySQLDialect{}
	expectedHostAliasQuery := "SELECT CONCAT(@@hostname, ':', @@port)"

	assert.Equal(t, expectedHostAliasQuery, testDatabaseDialect.GetHostAliasQuery())
}

func TestRdsMultiAzDbClusterMySQLDialect_GetServerVersion(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMultiAzDbClusterMySQLDialect{}
	expectedGetServerVersionQuery := "SHOW VARIABLES LIKE 'version_comment'"

	assert.Equal(t, expectedGetServerVersionQuery, testDatabaseDialect.GetServerVersionQuery())
}

func TestRdsMultiAzDbClusterMySQLDialect_GetHostListProvider(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMultiAzDbClusterMySQLDialect{}

	propsNoFailover := make(map[string]string)
	property_util.PLUGINS.Set(propsNoFailover, "efm")
	hostListProvider := testDatabaseDialect.GetHostListProvider(
		propsNoFailover,
		"dsn",
		nil,
		nil)

	rdsHostListProvider, ok := hostListProvider.(*driver_infrastructure.RdsHostListProvider)
	assert.True(t, ok, "expected an RdsHostListProvider to be returned")
	assert.NotNil(t, rdsHostListProvider)

	propsWithFailover := make(map[string]string)
	property_util.PLUGINS.Set(propsWithFailover, "failover")
	hostListProvider = testDatabaseDialect.GetHostListProvider(
		propsWithFailover,
		"dsn",
		nil,
		nil)

	monitoringHostListProvider, ok := hostListProvider.(*driver_infrastructure.MonitoringRdsHostListProvider)
	assert.True(t, ok, "expected a MonitoringRdsHostListProvider to be returned")
	assert.NotNil(t, monitoringHostListProvider)
}

func TestRdsMultiAzDbClusterMySQLDialect_GetHostRole(t *testing.T) {
	isReaderQuery := "SELECT @@innodb_read_only"
	testDatabaseDialect := &driver_infrastructure.RdsMultiAzDbClusterMySQLDialect{}
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
	mockRows.EXPECT().Columns().Return([]string{"1"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = int64(0)
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	assert.Equal(t, host_info_util.WRITER, testDatabaseDialect.GetHostRole(conn))

	// reader
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), isReaderQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"0"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = int64(1)
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

func TestRdsMultiAzDbClusterMySQLDialect_GetHostName(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMultiAzDbClusterMySQLDialect{}
	hostIdQuery := "SELECT endpoint from mysql.rds_topology as top where top.id = (SELECT @@server_id)"
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

	assert.Equal(t, instanceId, testDatabaseDialect.GetHostName(conn))

	// No Success
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), hostIdQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	assert.Equal(t, "", testDatabaseDialect.GetHostName(conn))
}

func TestRdsMultiAzDbClusterMySQLDialect_GetWriterHostName(t *testing.T) {
	testDatabaseDialect := &driver_infrastructure.RdsMultiAzDbClusterMySQLDialect{}

	hostId := int64(123456789)
	hostIdStr := strconv.FormatInt(hostId, 10)
	instanceId := "myinstance"
	instanceEndpoint := instanceId + ".com"
	writerHostIdQuery := "SHOW REPLICA STATUS"
	fetchEndpointQuery := fmt.Sprintf("SELECT endpoint from mysql.rds_topology as top "+
		"where top.id = '%v' and top.id = (SELECT @@server_id)", hostIdStr)

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

	// getWriterHostId Query
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), writerHostIdQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"Something", "Source_Server_Id"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = int64(123)
		dest[1] = int64(hostId)
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	// get endpoint query
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), fetchEndpointQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"host"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = []uint8(instanceEndpoint)
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	result, err := testDatabaseDialect.GetWriterHostName(conn)
	assert.NoError(t, err)
	assert.Equal(t, instanceId, result)

	// No Success
	// getWriterHostId Query
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), writerHostIdQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"Something", "Source_Server_Id"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = int64(123)
		dest[1] = int64(hostId)
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	// get endpoint query
	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), fetchEndpointQuery, gomock.Nil()).
		Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"host"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)
	result, err = testDatabaseDialect.GetWriterHostName(conn)
	assert.NoError(t, err)
	assert.Equal(t, "", result)
}

func TestRdsMultiAzDbClusterMySQLDialect_GetTopology(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockQueryer := mock_database_sql_driver.NewMockQueryerContext(ctrl)
	mockProvider := mock_driver_infrastructure.NewMockHostListProvider(ctrl)
	mockTopologyRows := mock_database_sql_driver.NewMockRows(ctrl)
	mockHostIdRows := mock_database_sql_driver.NewMockRows(ctrl)

	dialect := &driver_infrastructure.RdsMultiAzDbClusterMySQLDialect{}

	// Mock getting writer host id
	hostIdQuery := "SHOW REPLICA STATUS"
	expectedWriterId := int64(123456789)
	expectedReaderId := int64(132435465)
	expectedWriterHostName := "writerHostName"
	expectedReaderHostName := "readerHostName"
	expectedWriterEndpoint := expectedWriterHostName + ".com"
	expectedReaderEndpoint := expectedReaderHostName + ".com"

	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), hostIdQuery, gomock.Nil()).
		Return(mockHostIdRows, nil)

	mockHostIdRows.EXPECT().Columns().Return([]string{"NotId", "Source_Server_Id"})
	mockHostIdRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = "NotId"
		dest[1] = expectedWriterId
		return nil
	})
	mockHostIdRows.EXPECT().Close().Return(nil)

	// Mock Topology query
	topologyQuery := "SELECT id, endpoint FROM mysql.rds_topology"

	mockQueryer.EXPECT().
		QueryContext(gomock.Any(), topologyQuery, gomock.Nil()).
		Return(mockTopologyRows, nil)

	mockTopologyRows.EXPECT().Columns().Return([]string{"id", "endpoint"})

	mockTopologyRows.EXPECT().
		Next(gomock.Any()).
		DoAndReturn(func(dest []driver.Value) error {
			dest[0] = expectedWriterId
			dest[1] = []uint8(expectedWriterEndpoint)
			return nil
		})
	mockTopologyRows.EXPECT().
		Next(gomock.Any()).
		DoAndReturn(func(dest []driver.Value) error {
			dest[0] = expectedReaderId
			dest[1] = []uint8(expectedReaderEndpoint)
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
