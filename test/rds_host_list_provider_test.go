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
	"awssql/driver_infrastructure"
	"awssql/host_info_util"
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/assert"
)

var trueAsInt int64 = 1
var mockHostListProviderService = &MockRdsHostListProviderService{}
var emptyProps = map[string]string{}
var mockPgProps = map[string]string{"clusterId": "pg_cluster"}
var mockPgAuroraDialect = &driver_infrastructure.AuroraPgDatabaseDialect{}
var mockPgDsn = "postgres://someUser:somePassword@localhost:5432/pgx_test?sslmode=disable&foo=bar"
var mockPgRdsHostListProvider = driver_infrastructure.NewRdsHostListProvider(mockHostListProviderService, mockPgAuroraDialect, mockPgProps, mockPgDsn)

var mockMySQLProps = map[string]string{"clusterId": "mysql_cluster"}
var mockMySQLAuroraDialect = &driver_infrastructure.AuroraMySQLDatabaseDialect{}
var mockMySQLDsn = "someUser:somePassword@tcp(mydatabase.com:3306)/myDatabase?foo=bar&pop=snap"
var mockMySQLRdsHostListProvider = driver_infrastructure.NewRdsHostListProvider(mockHostListProviderService, mockMySQLAuroraDialect, mockMySQLProps, mockMySQLDsn)

func TestGetClusterId(t *testing.T) {
	pgClusterId := mockPgRdsHostListProvider.GetClusterId()
	if pgClusterId != "pg_cluster" {
		t.Errorf("Init should set cluster id to the value in mockPgProps.")
	}
	mysqlClusterId := mockMySQLRdsHostListProvider.GetClusterId()
	if mysqlClusterId != "mysql_cluster" {
		t.Errorf("Init should set cluster id to the value in mockMySQLProps.")
	}
}

func TestRefreshTopologyPg(t *testing.T) {
	// Test that Refresh returns the initial hosts from the dsn when topology query returns an empty host list.
	mockConn := MockConn{}
	hosts := mockPgRdsHostListProvider.Refresh(mockConn)
	if len(hosts) == 0 {
		t.Errorf("Refresh should return the initial hosts from the dsn if the topology query returns no hosts.")
	}
	if hosts[0].Host != "localhost" {
		t.Errorf("Refresh should correctly parse the dsn to create the initial hosts.")
	}

	// Test that Refresh returns the results of the topology query.
	mockConn.updateQueryRowSingleUse([]string{"hostName", "isWriter", "cpu", "lag", "lastUpdateTime"}, []driver.Value{"new_host", true, 1.0, 2.0, 0})

	hosts = mockPgRdsHostListProvider.Refresh(mockConn)
	if len(hosts) == 0 {
		t.Errorf("Refresh should return the results of the topology query.")
	}
	if hosts[0].Host != "new_host" || hosts[0].Role != host_info_util.WRITER || hosts[0].Weight != 201 {
		t.Errorf("Refresh should correctly parse the topology query to create the required hosts.")
	}

	// After the first topology query, topology is cached, Refresh should return results from cache instead of query when possible.
	mockConn = MockConn{}
	mockConn.updateQueryRowSingleUse([]string{"hostName", "isWriter", "cpu", "lag", "lastUpdateTime"}, []driver.Value{"topology_query_host", true, 1.0, 2.0, 0})

	hosts = mockPgRdsHostListProvider.Refresh(mockConn)
	if len(hosts) == 0 {
		t.Errorf("Refresh should return the cached topology.")
	}
	if hosts[0].Host == "topology_query_host" {
		t.Errorf("Refresh should return the cached topology if available.")
	}

	// ForceRefresh should run the topology query regardless of the values in the cache.
	hosts = mockPgRdsHostListProvider.ForceRefresh(mockConn)
	if len(hosts) == 0 {
		t.Errorf("ForceRefresh should return the results of the topology query.")
	}
	if hosts[0].Host != "topology_query_host" {
		t.Errorf("ForceRefresh should return the hosts from the topology query not the cached topology.")
	}
}

func TestRefreshTopologyMySQL(t *testing.T) {
	// Test that Refresh returns the initial hosts from the dsn when topology query returns an empty host list.
	basicConn := MockDriverConn{nil}
	hosts := mockMySQLRdsHostListProvider.Refresh(basicConn)
	if len(hosts) == 0 {
		t.Errorf("Refresh should return the initial hosts from the dsn if the topology query returns no hosts.")
	}
	if hosts[0].Host != "mydatabase.com" {
		t.Errorf("Refresh should correctly parse the dsn to create the initial hosts.")
	}

	// Test that Refresh returns the results of the topology query.
	mockConn := MockConn{}
	mockConn.updateQueryRowSingleUse([]string{"hostName", "isWriter", "cpu", "lag", "lastUpdateTime"},
		[]driver.Value{[]uint8{110, 101, 119, 95, 104, 111, 115, 116}, trueAsInt, 1.0, 2.0, 0})

	hosts = mockMySQLRdsHostListProvider.Refresh(mockConn)
	if len(hosts) == 0 {
		t.Errorf("Refresh should return the results of the topology query.")
	}
	if hosts[0].Host != "new_host" || hosts[0].Role != host_info_util.WRITER || hosts[0].Weight != 201 {
		t.Errorf("Refresh should correctly parse the topology query to create the required hosts.")
	}

	// After the first topology query, topology is cached, Refresh should return results from cache instead of query when possible.
	mockConn = MockConn{}
	mockConn.updateQueryRowSingleUse([]string{"hostName", "isWriter", "cpu", "lag", "lastUpdateTime"},
		[]driver.Value{[]uint8{116, 111, 112, 111, 108, 111, 103, 121, 95, 113, 117, 101, 114, 121, 95, 104, 111, 115, 116},
			trueAsInt, 1.0, 2.0, []uint8{50, 48, 48, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58, 48, 48, 46, 48, 48}})

	hosts = mockMySQLRdsHostListProvider.Refresh(mockConn)
	if len(hosts) == 0 {
		t.Errorf("Refresh should return the cached topology.")
	}
	if hosts[0].Host == "topology_query_host" {
		t.Errorf("Refresh should return the cached topology if available.")
	}

	// ForceRefresh should run the topology query regardless of the values in the cache.
	hosts = mockMySQLRdsHostListProvider.ForceRefresh(mockConn)
	if len(hosts) == 0 {
		t.Errorf("ForceRefresh should return the results of the topology query.")
	}
	if hosts[0].Host != "topology_query_host" {
		t.Errorf("ForceRefresh should return the hosts from the topology query not the cached topology.")
	}
}

func TestPgGetHostRole(t *testing.T) {
	mockConn := MockConn{}
	mockConn.updateQueryRow([]string{"isReader"}, []driver.Value{false})
	hostRole := mockPgRdsHostListProvider.GetHostRole(mockConn)
	if hostRole != host_info_util.WRITER {
		t.Errorf("When isReaderQuery returns false, getHostRole should return the WRITER HostRole.")
	}
}

func TestMySQLGetHostRole(t *testing.T) {
	mockConn := MockConn{}
	mockConn.updateQueryRow([]string{"isReader"}, []driver.Value{trueAsInt})
	hostRole := mockMySQLRdsHostListProvider.GetHostRole(mockConn)
	if hostRole != host_info_util.READER {
		t.Errorf("When isReaderQuery returns true, getHostRole should return the READER HostRole.")
	}
}

func TestPgIdentifyConnection(t *testing.T) {
	// Load the cache.
	mockConn := MockConn{}
	mockConn.updateQueryRowSingleUse([]string{"hostName", "isWriter", "cpu", "lag", "lastUpdateTime"}, []driver.Value{"topology_query_host", true, 1.0, 2.0, 0})
	mockPgRdsHostListProvider.Refresh(mockConn)

	mockConn = MockConn{}
	mockConn.updateQueryRow([]string{"hostId"}, []driver.Value{"topology_query_host"})
	currentConnection := mockPgRdsHostListProvider.IdentifyConnection(mockConn)
	if currentConnection.Host != "topology_query_host" {
		t.Errorf("Connection is attached to a host in the cache, returns that host.")
	}

	mockConn = MockConn{}
	mockConn.updateQueryRow([]string{"hostId"}, []driver.Value{"localhost"})
	currentConnection = mockPgRdsHostListProvider.IdentifyConnection(mockConn)
	if currentConnection.Host != "localhost" {
		t.Errorf("Connection is attached to a host not in the cache, calls forceRefresh and finds host and returns it.")
	}
}

func TestMySQLIdentifyConnection(t *testing.T) {
	// Load the cache.
	mockConn := MockConn{}
	mockConn.updateQueryRowSingleUse([]string{"hostName", "isWriter", "cpu", "lag", "lastUpdateTime"},
		[]driver.Value{[]uint8{116, 111, 112, 111, 108, 111, 103, 121, 95, 113, 117, 101, 114, 121, 95, 104, 111, 115, 116},
			trueAsInt, 1.0, 2.0, []uint8{50, 48, 48, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58, 48, 48, 46, 48, 48}})
	mockMySQLRdsHostListProvider.Refresh(mockConn)

	mockConn = MockConn{}
	mockConn.updateQueryRow([]string{"hostId"}, []driver.Value{[]uint8{116, 111, 112, 111, 108, 111, 103, 121, 95, 113, 117, 101, 114, 121, 95, 104, 111, 115, 116}})
	currentConnection := mockMySQLRdsHostListProvider.IdentifyConnection(mockConn)
	if currentConnection.Host != "topology_query_host" {
		t.Errorf("Connection is attached to a host in the cache, returns that host.")
	}

	mockConn = MockConn{}
	mockConn.updateQueryRow([]string{"hostId"}, []driver.Value{[]uint8{109, 121, 100, 97, 116, 97, 98, 97, 115, 101, 46, 99, 111, 109}})
	currentConnection = mockMySQLRdsHostListProvider.IdentifyConnection(mockConn)
	if currentConnection.Host != "mydatabase.com" {
		t.Errorf("Connection is attached to a host not in the cache, calls forceRefresh and finds host and returns it.")
	}
}

func TestSuggestedClusterIdForRds(t *testing.T) {
	driver_infrastructure.ClearAllRdsHostListProviderCaches()

	dsn := "postgresql://user:password@name.cluster-xyz.us-east-2.rds.amazonaws.com:5432/database"
	provider1 := driver_infrastructure.NewRdsHostListProvider(mockHostListProviderService, mockPgAuroraDialect, emptyProps, dsn)
	mockConn := MockConn{}
	mockConn.updateQueryRowSingleUse([]string{"hostName", "isWriter", "cpu", "lag", "lastUpdateTime"},
		[]driver.Value{"instance-a-1.xyz.us-east-2.rds.amazonaws.com", true, 1.0, 2.0, 0})

	assert.Equal(t, 0, driver_infrastructure.TopologyCache.Size())
	hosts := provider1.Refresh(mockConn)
	assert.Equal(t, "instance-a-1.xyz.us-east-2.rds.amazonaws.com.cluster-", hosts[0].Host)

	provider2 := driver_infrastructure.NewRdsHostListProvider(mockHostListProviderService, mockPgAuroraDialect, emptyProps, dsn)
	assert.Equal(t, provider1.GetClusterId(), provider2.GetClusterId())
	assert.True(t, provider1.IsPrimaryClusterId)
	assert.True(t, provider2.IsPrimaryClusterId)

	hosts = provider2.Refresh(MockDriverConn{})
	assert.Equal(t, "instance-a-1.xyz.us-east-2.rds.amazonaws.com.cluster-", hosts[0].Host)
	assert.Equal(t, 1, driver_infrastructure.TopologyCache.Size())
}

func TestNoSuggestedClusterId(t *testing.T) {
	driver_infrastructure.ClearAllRdsHostListProviderCaches()

	dsn1 := "postgresql://user:password@name1.cluster-xyz.us-east-2.rds.amazonaws.com:5432/database"
	provider1 := driver_infrastructure.NewRdsHostListProvider(mockHostListProviderService, mockPgAuroraDialect, emptyProps, dsn1)
	mockConn := MockConn{}
	mockConn.updateQueryRowSingleUse([]string{"hostName", "isWriter", "cpu", "lag", "lastUpdateTime"},
		[]driver.Value{"instance-a-1.xyz.us-east-2.rds.amazonaws.com", true, 1.0, 2.0, 0})

	assert.Equal(t, 0, driver_infrastructure.TopologyCache.Size())
	hosts := provider1.Refresh(mockConn)
	assert.Equal(t, "instance-a-1.xyz.us-east-2.rds.amazonaws.com.cluster-", hosts[0].Host)

	dsn2 := "postgresql://user:password@name2.cluster-xyz.us-east-2.rds.amazonaws.com:5432/database"
	provider2 := driver_infrastructure.NewRdsHostListProvider(mockHostListProviderService, mockPgAuroraDialect, emptyProps, dsn2)
	mockConn = MockConn{}
	mockConn.updateQueryRowSingleUse([]string{"hostName", "isWriter", "cpu", "lag", "lastUpdateTime"},
		[]driver.Value{"instance-b-1.xyz.us-east-2.rds.amazonaws.com", true, 1.0, 2.0, 0})

	hosts = provider2.Refresh(mockConn)
	assert.NotEqual(t, provider1.GetClusterId(), provider2.GetClusterId())
	assert.True(t, provider1.IsPrimaryClusterId)
	assert.True(t, provider2.IsPrimaryClusterId)
	assert.Equal(t, "instance-b-1.xyz.us-east-2.rds.amazonaws.com.cluster-", hosts[0].Host)
	assert.Equal(t, 2, driver_infrastructure.TopologyCache.Size())
}

type MockRdsHostListProviderService struct {
}

func (m *MockRdsHostListProviderService) GetDialect() driver_infrastructure.DatabaseDialect {
	panic("unimplemented")
}

func (m *MockRdsHostListProviderService) GetHostListProvider() driver_infrastructure.HostListProvider {
	panic("unimplemented")
}

func (m *MockRdsHostListProviderService) GetInitialConnectionHostInfo() host_info_util.HostInfo {
	panic("unimplemented")
}

func (m *MockRdsHostListProviderService) SetHostListProvider(hostListProvider driver_infrastructure.HostListProvider) {
	panic("unimplemented")
}

func (m *MockRdsHostListProviderService) SetInitialConnectionHostInfo(info host_info_util.HostInfo) {
	panic("unimplemented")
}

func (m *MockRdsHostListProviderService) GetCurrentConnection() *driver.Conn {
	return nil
}

func (m *MockRdsHostListProviderService) GetHostInfoBuilder() *host_info_util.HostInfoBuilder {
	return host_info_util.NewHostInfoBuilder()
}
