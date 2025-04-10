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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

var trueAsInt int64 = 1
var mockHostListProviderService = &MockRdsHostListProviderService{}
var emptyProps = map[string]string{}
var mockPgAuroraDialect = &driver_infrastructure.AuroraPgDatabaseDialect{}

func beforePgTests() *driver_infrastructure.RdsHostListProvider {
	driver_infrastructure.ClearAllRdsHostListProviderCaches()
	var mockPgProps = map[string]string{"clusterId": "pg_cluster"}
	var mockPgDsn = "postgres://someUser:somePassword@localhost:5432/pgx_test?sslmode=disable&foo=bar"
	return driver_infrastructure.NewRdsHostListProvider(mockHostListProviderService, mockPgAuroraDialect, mockPgProps, mockPgDsn)
}
func beforeMySqlTests() *driver_infrastructure.RdsHostListProvider {
	driver_infrastructure.ClearAllRdsHostListProviderCaches()
	mockMySQLProps := map[string]string{"clusterId": "mysql_cluster"}
	mockMySQLDsn := "someUser:somePassword@tcp(mydatabase.com:3306)/myDatabase?foo=bar&pop=snap"
	return driver_infrastructure.NewRdsHostListProvider(mockHostListProviderService, &driver_infrastructure.AuroraMySQLDatabaseDialect{}, mockMySQLProps, mockMySQLDsn)
}

func TestGetClusterId(t *testing.T) {
	mockPgRdsHostListProvider := beforePgTests()
	pgClusterId := mockPgRdsHostListProvider.GetClusterId()
	if pgClusterId != "pg_cluster" {
		t.Errorf("Init should set cluster id to the value in mockPgProps.")
	}
	mockMySQLRdsHostListProvider := beforeMySqlTests()
	mysqlClusterId := mockMySQLRdsHostListProvider.GetClusterId()
	if mysqlClusterId != "mysql_cluster" {
		t.Errorf("Init should set cluster id to the value in mockMySQLProps.")
	}
}

func TestRefreshTopologyPg(t *testing.T) {
	mockPgRdsHostListProvider := beforePgTests()
	// Test that Refresh returns the initial hosts from the dsn when topology query returns an empty host list.
	mockConn := MockConn{}
	hosts, err := mockPgRdsHostListProvider.Refresh(&mockConn)
	assert.Nil(t, err)
	if len(hosts) == 0 {
		t.Errorf("Refresh should return the initial hosts from the dsn if the topology query returns no hosts.")
	}
	if hosts[0].Host != "localhost" {
		t.Errorf("Refresh should correctly parse the dsn to create the initial hosts.")
	}

	// Test that Refresh returns the results of the topology query.
	mockConn.updateQueryRowSingleUse([]string{"hostName", "isWriter", "cpu", "lag", "lastUpdateTime"}, []driver.Value{"new_host", true, 1.0, 2.0, 0})

	hosts, err = mockPgRdsHostListProvider.Refresh(&mockConn)
	assert.Nil(t, err)
	if len(hosts) == 0 {
		t.Errorf("Refresh should return the results of the topology query.")
	}
	if hosts[0].Host != "new_host" || hosts[0].Role != host_info_util.WRITER || hosts[0].Weight != 201 {
		t.Errorf("Refresh should correctly parse the topology query to create the required hosts.")
	}

	// After the first topology query, topology is cached, Refresh should return results from cache instead of query when possible.
	mockConn = MockConn{}
	mockConn.updateQueryRowSingleUse([]string{"hostName", "isWriter", "cpu", "lag", "lastUpdateTime"}, []driver.Value{"topology_query_host", true, 1.0, 2.0, 0})

	hosts, err = mockPgRdsHostListProvider.Refresh(&mockConn)
	assert.Nil(t, err)
	if len(hosts) == 0 {
		t.Errorf("Refresh should return the cached topology.")
	}
	if hosts[0].Host == "topology_query_host" {
		t.Errorf("Refresh should return the cached topology if available.")
	}

	// ForceRefresh should run the topology query regardless of the values in the cache.
	hosts, err = mockPgRdsHostListProvider.ForceRefresh(&mockConn)
	assert.Nil(t, err)
	if len(hosts) == 0 {
		t.Errorf("ForceRefresh should return the results of the topology query.")
	}
	if hosts[0].Host != "topology_query_host" {
		t.Errorf("ForceRefresh should return the hosts from the topology query not the cached topology.")
	}
}

func TestRefreshTopologyMySQL(t *testing.T) {
	// Test that Refresh returns the error when a query fails.
	mockMySQLRdsHostListProvider := beforeMySqlTests()
	basicConn := MockDriverConn{nil}
	_, err := mockMySQLRdsHostListProvider.Refresh(basicConn)
	if !strings.Contains(err.Error(), "does not implement the required interface") {
		t.Errorf("If the given connection does not implement QueryerContext it should error out.")
	}

	// Test that Refresh returns the initial hosts from the dsn when topology query returns an empty host list.
	mockConn := MockConn{}
	mockConn.updateQueryRowSingleUse([]string{}, []driver.Value{})
	hosts, err := mockMySQLRdsHostListProvider.Refresh(&mockConn)
	assert.Nil(t, err)

	if hosts[0].Host != "mydatabase.com" {
		t.Errorf("Refresh should correctly parse the dsn to create the initial hosts.")
	}

	// Test that Refresh returns the results of the topology query.
	mockConn = MockConn{}
	mockConn.updateQueryRowSingleUse([]string{"hostName", "isWriter", "cpu", "lag", "lastUpdateTime"},
		[]driver.Value{[]uint8{110, 101, 119, 95, 104, 111, 115, 116}, trueAsInt, 1.0, 2.0, 0})

	hosts, err = mockMySQLRdsHostListProvider.Refresh(&mockConn)
	assert.Nil(t, err)
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

	hosts, err = mockMySQLRdsHostListProvider.Refresh(&mockConn)
	assert.Nil(t, err)
	if len(hosts) == 0 {
		t.Errorf("Refresh should return the cached topology.")
	}
	if hosts[0].Host == "topology_query_host" {
		t.Errorf("Refresh should return the cached topology if available.")
	}

	// ForceRefresh should run the topology query regardless of the values in the cache.
	hosts, err = mockMySQLRdsHostListProvider.ForceRefresh(&mockConn)
	assert.Nil(t, err)
	if len(hosts) == 0 {
		t.Errorf("ForceRefresh should return the results of the topology query.")
	}
	if hosts[0].Host != "topology_query_host" {
		t.Errorf("ForceRefresh should return the hosts from the topology query not the cached topology.")
	}
}

func TestPgGetHostRole(t *testing.T) {
	mockPgRdsHostListProvider := beforePgTests()
	mockConn := MockConn{}
	mockConn.updateQueryRow([]string{"isReader"}, []driver.Value{false})
	hostRole := mockPgRdsHostListProvider.GetHostRole(&mockConn)
	if hostRole != host_info_util.WRITER {
		t.Errorf("When isReaderQuery returns false, getHostRole should return the WRITER HostRole.")
	}
}

func TestMySQLGetHostRole(t *testing.T) {
	mockConn := MockConn{}
	mockConn.updateQueryRow([]string{"isReader"}, []driver.Value{trueAsInt})
	mockMySQLRdsHostListProvider := beforeMySqlTests()
	hostRole := mockMySQLRdsHostListProvider.GetHostRole(&mockConn)
	if hostRole != host_info_util.READER {
		t.Errorf("When isReaderQuery returns true, getHostRole should return the READER HostRole.")
	}
}

func TestPgIdentifyConnection(t *testing.T) {
	mockPgRdsHostListProvider := beforePgTests()
	mockConn := MockConn{}
	mockConn.updateQueryRow([]string{"hostId"}, []driver.Value{"localhost"})
	currentConnection, err := mockPgRdsHostListProvider.IdentifyConnection(&mockConn)
	assert.Nil(t, err)
	if currentConnection.Host != "localhost" {
		t.Errorf("Connection is attached to a host not in the cache, should call forceRefresh and finds host and returns it.")
	}

	// Load the cache.
	mockConn = MockConn{}
	mockConn.updateQueryRowSingleUse([]string{"hostName", "isWriter", "cpu", "lag", "lastUpdateTime"}, []driver.Value{"topology_query_host", true, 1.0, 2.0, 0})
	_, err = mockPgRdsHostListProvider.Refresh(&mockConn)
	assert.Nil(t, err)

	mockConn = MockConn{}
	mockConn.updateQueryRow([]string{"hostId"}, []driver.Value{"topology_query_host"})
	currentConnection, err = mockPgRdsHostListProvider.IdentifyConnection(&mockConn)
	assert.Nil(t, err)
	if currentConnection.Host != "topology_query_host" {
		t.Errorf("Connection is attached to a host in the cache, should return that host.")
	}
}

func TestMySQLIdentifyConnection(t *testing.T) {
	mockMySQLRdsHostListProvider := beforeMySqlTests()
	mockConn := MockConn{}
	mockConn.updateQueryRow([]string{"hostId"}, []driver.Value{[]uint8{109, 121, 100, 97, 116, 97, 98, 97, 115, 101, 46, 99, 111, 109}})
	currentConnection, err := mockMySQLRdsHostListProvider.IdentifyConnection(&mockConn)
	assert.Nil(t, err)
	if currentConnection.Host != "mydatabase.com" {
		t.Errorf("Connection is attached to a host not in the cache, should call forceRefresh and finds host and returns it.")
	}

	// Load the cache.
	mockConn = MockConn{}
	mockConn.updateQueryRowSingleUse([]string{"hostName", "isWriter", "cpu", "lag", "lastUpdateTime"},
		[]driver.Value{[]uint8{116, 111, 112, 111, 108, 111, 103, 121, 95, 113, 117, 101, 114, 121, 95, 104, 111, 115, 116},
			trueAsInt, 1.0, 2.0, []uint8{50, 48, 48, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58, 48, 48, 46, 48, 48}})
	_, err = mockMySQLRdsHostListProvider.Refresh(&mockConn)
	assert.Nil(t, err)

	mockConn = MockConn{}
	mockConn.updateQueryRow([]string{"hostId"}, []driver.Value{[]uint8{116, 111, 112, 111, 108, 111, 103, 121, 95, 113, 117, 101, 114, 121, 95, 104, 111, 115, 116}})
	currentConnection, err = mockMySQLRdsHostListProvider.IdentifyConnection(&mockConn)
	assert.Nil(t, err)
	if currentConnection.Host != "topology_query_host" {
		t.Errorf("Connection is attached to a host in the cache, should return that host.")
	}
}

func TestSuggestedClusterIdForRds(t *testing.T) {
	driver_infrastructure.ClearAllRdsHostListProviderCaches()

	dsn := "postgresql://user:password@name.cluster-xyz.us-east-2.rds.amazonaws.com:5432/database"
	provider1 := driver_infrastructure.NewRdsHostListProvider(mockHostListProviderService, mockPgAuroraDialect, emptyProps, dsn)
	mockConn := MockConn{}
	mockConn.updateQueryRowSingleUse([]string{"hostName", "isWriter", "cpu", "lag", "lastUpdateTime"},
		[]driver.Value{"instance-a-1", true, 1.0, 2.0, 0})

	assert.Equal(t, 0, driver_infrastructure.TopologyCache.Size())
	hosts, err := provider1.Refresh(&mockConn)
	assert.Nil(t, err)
	assert.Equal(t, "instance-a-1.xyz.us-east-2.rds.amazonaws.com", hosts[0].Host)

	provider2 := driver_infrastructure.NewRdsHostListProvider(mockHostListProviderService, mockPgAuroraDialect, emptyProps, dsn)
	assert.Equal(t, provider1.GetClusterId(), provider2.GetClusterId())
	assert.True(t, provider1.IsPrimaryClusterId)
	assert.True(t, provider2.IsPrimaryClusterId)

	hosts, err = provider2.Refresh(MockDriverConn{})
	assert.Nil(t, err)
	assert.Equal(t, "instance-a-1.xyz.us-east-2.rds.amazonaws.com", hosts[0].Host)
	assert.Equal(t, 1, driver_infrastructure.TopologyCache.Size())
}

func TestNoSuggestedClusterId(t *testing.T) {
	driver_infrastructure.ClearAllRdsHostListProviderCaches()

	dsn1 := "postgresql://user:password@name1.cluster-xyz.us-east-2.rds.amazonaws.com:5432/database"
	provider1 := driver_infrastructure.NewRdsHostListProvider(mockHostListProviderService, mockPgAuroraDialect, emptyProps, dsn1)
	mockConn := MockConn{}
	mockConn.updateQueryRowSingleUse([]string{"hostName", "isWriter", "cpu", "lag", "lastUpdateTime"},
		[]driver.Value{"instance-a-1", true, 1.0, 2.0, 0})

	assert.Equal(t, 0, driver_infrastructure.TopologyCache.Size())
	hosts, err := provider1.Refresh(&mockConn)
	assert.Nil(t, err)
	assert.Equal(t, "instance-a-1.xyz.us-east-2.rds.amazonaws.com", hosts[0].Host)

	dsn2 := "postgresql://user:password@name2.cluster-xyz.us-east-2.rds.amazonaws.com:5432/database"
	provider2 := driver_infrastructure.NewRdsHostListProvider(mockHostListProviderService, mockPgAuroraDialect, emptyProps, dsn2)
	mockConn = MockConn{}
	mockConn.updateQueryRowSingleUse([]string{"hostName", "isWriter", "cpu", "lag", "lastUpdateTime"},
		[]driver.Value{"instance-b-1", true, 1.0, 2.0, 0})

	hosts, err = provider2.Refresh(&mockConn)
	assert.Nil(t, err)
	assert.NotEqual(t, provider1.GetClusterId(), provider2.GetClusterId())
	assert.True(t, provider1.IsPrimaryClusterId)
	assert.True(t, provider2.IsPrimaryClusterId)
	assert.Equal(t, "instance-b-1.xyz.us-east-2.rds.amazonaws.com", hosts[0].Host)
	assert.Equal(t, 2, driver_infrastructure.TopologyCache.Size())
}
