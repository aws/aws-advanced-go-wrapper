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
	"strings"
	"testing"
	"time"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/services"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

// rdsTestSetup creates the common gomock infrastructure for RdsHostListProvider tests.
// The caller is responsible for setting IsDialectConfirmed expectations on the returned MockPluginService.
func rdsTestSetup(
	ctrl *gomock.Controller,
	dsn string,
) (
	*driver_infrastructure.RdsHostListProvider,
	*mock_driver_infrastructure.MockPluginService,
	*mock_driver_infrastructure.MockMonitorService,
	*mock_driver_infrastructure.MockClusterTopologyUtils,
	*mock_driver_infrastructure.MockServicesContainer,
	*services.ExpiringStorage,
) {
	props, _ := property_util.ParseDsn(dsn)

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)

	mockMonitorService := mock_driver_infrastructure.NewMockMonitorService(ctrl)
	mockMonitorService.EXPECT().StopAndRemoveByType(gomock.Any()).AnyTimes()

	storageService := services.NewExpiringStorage(5*time.Minute, nil)
	driver_infrastructure.RegisterDefaultStorageTypes(storageService)

	mockHLPService := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)
	mockHLPService.EXPECT().SetInitialConnectionHostInfo(gomock.Any()).AnyTimes()
	mockHLPService.EXPECT().GetDialect().Return(nil).AnyTimes()

	mockTopologyUtils := mock_driver_infrastructure.NewMockClusterTopologyUtils(ctrl)

	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()
	mockContainer.EXPECT().GetMonitorService().Return(mockMonitorService).AnyTimes()
	mockContainer.EXPECT().GetStorageService().Return(storageService).AnyTimes()

	provider := driver_infrastructure.NewRdsHostListProvider(mockHLPService, mockTopologyUtils, props, mockContainer, nil)

	return provider, mockPluginService, mockMonitorService, mockTopologyUtils, mockContainer, storageService
}

func TestGetClusterId(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pgProvider, mockPS1, _, _, _, _ := rdsTestSetup(ctrl,
		"postgres://someUser:somePassword@localhost:5432/pgx_test?sslmode=disable&foo=bar&clusterId=pg_cluster")
	mockPS1.EXPECT().IsDialectConfirmed().Return(false).AnyTimes()
	pgClusterId, _ := pgProvider.GetClusterId()
	assert.Equal(t, "pg_cluster", pgClusterId)

	mysqlProvider, mockPS2, _, _, _, _ := rdsTestSetup(ctrl,
		"someUser:somePassword@tcp(mydatabase.com:3306)/myDatabase?foo=bar&pop=snap&clusterId=mysql_cluster")
	mockPS2.EXPECT().IsDialectConfirmed().Return(false).AnyTimes()
	mysqlClusterId, _ := mysqlProvider.GetClusterId()
	assert.Equal(t, "mysql_cluster", mysqlClusterId)
}

func TestRefreshReturnsInitialHostsWhenDialectNotConfirmed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	provider, mockPS, _, _, _, _ := rdsTestSetup(ctrl,
		"postgres://someUser:somePassword@localhost:5432/pgx_test?sslmode=disable&foo=bar&clusterId=pg_cluster")
	mockPS.EXPECT().IsDialectConfirmed().Return(false).AnyTimes()

	hosts, err := provider.Refresh()
	assert.Nil(t, err)
	assert.NotEmpty(t, hosts)
	assert.Equal(t, "localhost", hosts[0].Host)
}

func TestForceRefreshReturnsInitialHostsWhenDialectNotConfirmed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	provider, mockPS, _, _, _, _ := rdsTestSetup(ctrl,
		"postgres://someUser:somePassword@localhost:5432/pgx_test?sslmode=disable&foo=bar&clusterId=pg_cluster")
	mockPS.EXPECT().IsDialectConfirmed().Return(false).AnyTimes()

	hosts, err := provider.ForceRefresh()
	assert.Nil(t, err)
	assert.NotEmpty(t, hosts)
	assert.Equal(t, "localhost", hosts[0].Host)
}

func TestRefreshReturnsCachedTopologyWhenAvailable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	provider, mockPS, _, _, _, storageService := rdsTestSetup(ctrl,
		"postgres://someUser:somePassword@localhost:5432/pgx_test?sslmode=disable&foo=bar&clusterId=pg_cluster")
	mockPS.EXPECT().IsDialectConfirmed().Return(true).AnyTimes()

	// Pre-populate the topology cache.
	cachedHost, _ := host_info_util.NewHostInfoBuilder().SetHost("cached_host").SetRole(host_info_util.WRITER).Build()
	driver_infrastructure.TopologyStorageType.Set(storageService, "pg_cluster",
		driver_infrastructure.NewTopology([]*host_info_util.HostInfo{cachedHost}))

	hosts, err := provider.Refresh()
	assert.Nil(t, err)
	assert.NotEmpty(t, hosts)
	assert.Equal(t, "cached_host", hosts[0].Host)
}

func TestRefreshQueriesMonitorWhenNoCachedTopology(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	provider, mockPS, mockMonitorService, _, mockContainer, _ := rdsTestSetup(ctrl,
		"postgres://someUser:somePassword@localhost:5432/pgx_test?sslmode=disable&foo=bar&clusterId=pg_cluster")
	mockPS.EXPECT().IsDialectConfirmed().Return(true).AnyTimes()

	// Mock the driver dialect + resolver chain for getMonitoringProps.
	mockDriverDialect := newMockDriverDialect(ctrl, nil)
	mockPS.EXPECT().GetTargetDriverDialect().Return(mockDriverDialect).AnyTimes()

	// Mock the monitor to return topology hosts.
	monitorHost, _ := host_info_util.NewHostInfoBuilder().SetHost("monitor_host").SetRole(host_info_util.WRITER).Build()
	mockMonitor := mock_driver_infrastructure.NewMockClusterTopologyMonitor(ctrl)
	mockMonitor.EXPECT().ForceRefresh(false, driver_infrastructure.DEFAULT_TOPOLOGY_QUERY_TIMEOUT_MS).
		Return([]*host_info_util.HostInfo{monitorHost}, nil)

	mockMonitorService.EXPECT().RunIfAbsent(
		driver_infrastructure.ClusterTopologyMonitorType,
		"pg_cluster",
		mockContainer,
		gomock.Any(),
	).Return(mockMonitor, nil)

	hosts, err := provider.Refresh()
	assert.Nil(t, err)
	assert.NotEmpty(t, hosts)
	assert.Equal(t, "monitor_host", hosts[0].Host)
}

func TestForceRefreshAlwaysQueriesMonitor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	provider, mockPS, mockMonitorService, _, mockContainer, storageService := rdsTestSetup(ctrl,
		"postgres://someUser:somePassword@localhost:5432/pgx_test?sslmode=disable&foo=bar&clusterId=pg_cluster")
	mockPS.EXPECT().IsDialectConfirmed().Return(true).AnyTimes()

	// Mock the driver dialect + resolver chain for getMonitoringProps.
	mockDriverDialect := newMockDriverDialect(ctrl, nil)
	mockPS.EXPECT().GetTargetDriverDialect().Return(mockDriverDialect).AnyTimes()

	// Pre-populate cache with old data.
	cachedHost, _ := host_info_util.NewHostInfoBuilder().SetHost("cached_host").SetRole(host_info_util.WRITER).Build()
	driver_infrastructure.TopologyStorageType.Set(storageService, "pg_cluster",
		driver_infrastructure.NewTopology([]*host_info_util.HostInfo{cachedHost}))

	// Mock the monitor to return fresh topology.
	freshHost, _ := host_info_util.NewHostInfoBuilder().SetHost("fresh_host").SetRole(host_info_util.WRITER).Build()
	mockMonitor := mock_driver_infrastructure.NewMockClusterTopologyMonitor(ctrl)
	mockMonitor.EXPECT().ForceRefresh(false, driver_infrastructure.DEFAULT_TOPOLOGY_QUERY_TIMEOUT_MS).
		Return([]*host_info_util.HostInfo{freshHost}, nil)

	mockMonitorService.EXPECT().RunIfAbsent(
		driver_infrastructure.ClusterTopologyMonitorType,
		"pg_cluster",
		mockContainer,
		gomock.Any(),
	).Return(mockMonitor, nil)

	hosts, err := provider.ForceRefresh()
	assert.Nil(t, err)
	assert.NotEmpty(t, hosts)
	assert.Equal(t, "fresh_host", hosts[0].Host)
}

func TestGetHostRole(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	provider, mockPS, _, mockTopologyUtils, _, _ := rdsTestSetup(ctrl,
		"postgres://someUser:somePassword@localhost:5432/pgx_test?sslmode=disable&foo=bar&clusterId=pg_cluster")
	mockPS.EXPECT().IsDialectConfirmed().Return(false).AnyTimes()

	mockConn := &MockConn{}

	// GetHostRole delegates to TopologyUtils.
	mockTopologyUtils.EXPECT().GetHostRole(mockConn).Return(host_info_util.WRITER)
	hostRole := provider.GetHostRole(mockConn)
	assert.Equal(t, host_info_util.WRITER, hostRole)

	mockTopologyUtils.EXPECT().GetHostRole(mockConn).Return(host_info_util.READER)
	hostRole = provider.GetHostRole(mockConn)
	assert.Equal(t, host_info_util.READER, hostRole)
}

func TestRdsIdentifyConnection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	provider, mockPS, _, mockTopologyUtils, _, storageService := rdsTestSetup(ctrl,
		"postgres://someUser:somePassword@localhost:5432/pgx_test?sslmode=disable&foo=bar&clusterId=pg_cluster")

	mockConn := &MockConn{}

	// Pre-populate cache with a host that has hostId set, since IdentifyConnection now matches by hostId.
	initialHost, _ := host_info_util.NewHostInfoBuilder().SetHost("localhost").SetHostId("localhost").SetRole(host_info_util.WRITER).Build()
	driver_infrastructure.TopologyStorageType.Set(storageService, "pg_cluster",
		driver_infrastructure.NewTopology([]*host_info_util.HostInfo{initialHost}))

	mockPS.EXPECT().IsDialectConfirmed().Return(true).AnyTimes()
	mockTopologyUtils.EXPECT().GetInstanceId(mockConn).Return("localhost", "localhost")

	currentConnection, err := provider.IdentifyConnection(mockConn)
	assert.Nil(t, err)
	assert.Equal(t, "localhost", currentConnection.Host)

	// Now update cache with a different host.
	cachedHost, _ := host_info_util.NewHostInfoBuilder().SetHost("cached_host").SetHostId("cached_host").SetRole(host_info_util.WRITER).Build()
	driver_infrastructure.TopologyStorageType.Set(storageService, "pg_cluster",
		driver_infrastructure.NewTopology([]*host_info_util.HostInfo{cachedHost}))

	mockTopologyUtils.EXPECT().GetInstanceId(mockConn).Return("cached_host", "cached_host")

	currentConnection, err = provider.IdentifyConnection(mockConn)
	assert.Nil(t, err)
	assert.Equal(t, "cached_host", currentConnection.Host)
}

func TestRdsIdentifyConnectionNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	provider, mockPS, _, mockTopologyUtils, _, _ := rdsTestSetup(ctrl,
		"postgres://someUser:somePassword@localhost:5432/pgx_test?sslmode=disable&foo=bar&clusterId=pg_cluster")
	mockPS.EXPECT().IsDialectConfirmed().Return(false).AnyTimes()

	mockConn := &MockConn{}

	// When GetInstanceId returns empty strings, IdentifyConnection should error.
	mockTopologyUtils.EXPECT().GetInstanceId(mockConn).Return("", "")

	_, err := provider.IdentifyConnection(mockConn)
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "Unable"))
}

func TestSuggestedClusterIdForRds(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Two providers with the same clusterId should share the same cluster ID.
	provider1, mockPS1, _, _, _, _ := rdsTestSetup(ctrl,
		"postgresql://user:password@name.cluster-xyz.us-east-2.rds.amazonaws.com:5432/database?clusterId=shared_cluster")
	mockPS1.EXPECT().IsDialectConfirmed().Return(false).AnyTimes()

	provider2, mockPS2, _, _, _, _ := rdsTestSetup(ctrl,
		"postgresql://user:password@name.cluster-xyz.us-east-2.rds.amazonaws.com:5432/database?clusterId=shared_cluster")
	mockPS2.EXPECT().IsDialectConfirmed().Return(false).AnyTimes()

	actualClusterId1, err1 := provider1.GetClusterId()
	actualClusterId2, err2 := provider2.GetClusterId()
	assert.Nil(t, err1)
	assert.Nil(t, err2)
	assert.Equal(t, actualClusterId1, actualClusterId2)
	assert.Equal(t, "shared_cluster", actualClusterId1)
}

func TestNoSuggestedClusterId(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Two providers with different clusterIds should have different cluster IDs.
	provider1, mockPS1, _, _, _, _ := rdsTestSetup(ctrl,
		"postgresql://user:password@name1.cluster-xyz.us-east-2.rds.amazonaws.com:5432/database?clusterId=cluster_1")
	mockPS1.EXPECT().IsDialectConfirmed().Return(false).AnyTimes()

	provider2, mockPS2, _, _, _, _ := rdsTestSetup(ctrl,
		"postgresql://user:password@name2.cluster-xyz.us-east-2.rds.amazonaws.com:5432/database?clusterId=cluster_2")
	mockPS2.EXPECT().IsDialectConfirmed().Return(false).AnyTimes()

	actualClusterId1, err1 := provider1.GetClusterId()
	actualClusterId2, err2 := provider2.GetClusterId()
	assert.Nil(t, err1)
	assert.Nil(t, err2)
	assert.NotEqual(t, actualClusterId1, actualClusterId2)
}

func TestClearAllRdsHostListProviderCaches(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMonitorService := mock_driver_infrastructure.NewMockMonitorService(ctrl)
	mockMonitorService.EXPECT().StopAndRemoveByType(driver_infrastructure.ClusterTopologyMonitorType).Times(1)

	driver_infrastructure.ClearAllRdsHostListProviderCaches(mockMonitorService)
}
