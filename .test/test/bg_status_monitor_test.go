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
	"errors"
	"testing"
	"time"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	mock_database_sql_driver "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/database_sql_driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/bg"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestNewBlueGreenStatusMonitor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)

	hostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("test-host").SetPort(3306).Build()

	statusCheckIntervalMap := map[driver_infrastructure.BlueGreenIntervalRate]int{
		driver_infrastructure.BASELINE:  300000,
		driver_infrastructure.INCREASED: 60000,
		driver_infrastructure.HIGH:      5000,
	}

	callbackCalled := false
	onStatusChangeFunc := func(role driver_infrastructure.BlueGreenRole, interimStatus bg.BlueGreenInterimStatus) {
		callbackCalled = true
	}

	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()

	monitor := bg.NewTestBlueGreenStatusMonitor(
		driver_infrastructure.SOURCE,
		"test-bg-id",
		hostInfo,
		mockPluginService,
		emptyProps,
		statusCheckIntervalMap,
		onStatusChangeFunc,
	)

	assert.NotNil(t, monitor)
	assert.False(t, callbackCalled)

	// Test getting initial values
	topology := monitor.GetCurrentTopology()
	assert.NotNil(t, topology)
	assert.Len(t, topology, 0)

	ip := monitor.GetConnectedIpAddress()
	assert.Equal(t, "", ip)
	monitor.SetConnectedIpAddress("localhost")
	assert.Equal(t, "localhost", monitor.GetConnectedIpAddress())

	// Test setting and getting interval rate.
	assert.Equal(t, driver_infrastructure.BASELINE, monitor.GetIntervalRate())
	monitor.SetIntervalRate(driver_infrastructure.INCREASED)
	assert.Equal(t, driver_infrastructure.INCREASED, monitor.GetIntervalRate())
	monitor.SetIntervalRate(driver_infrastructure.HIGH)
	assert.Equal(t, driver_infrastructure.HIGH, monitor.GetIntervalRate())

	// Test getting ip addresses from hosts
	ip = monitor.GetIpAddress("localhost")
	assert.NotEmpty(t, ip)

	ip = monitor.GetIpAddress("invalid-host-that-does-not-exist.example.com")
	assert.Empty(t, ip)
}

func TestBlueGreenStatusMonitorDelay(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)

	mockPluginService.EXPECT().GetDialect().Return(&driver_infrastructure.MySQLDatabaseDialect{}).AnyTimes()

	monitor := bg.NewTestBlueGreenStatusMonitor(
		driver_infrastructure.SOURCE,
		"test-bg-id",
		nil,
		mockPluginService,
		nil,
		map[driver_infrastructure.BlueGreenIntervalRate]int{
			driver_infrastructure.BASELINE:  120,
			driver_infrastructure.INCREASED: 60,
			driver_infrastructure.HIGH:      45,
		}, nil)

	start := time.Now()
	monitor.Delay()
	elapsed := time.Since(start)

	assert.True(t, elapsed < 40*time.Millisecond, "Should not delay if monitor is set to stop.")

	monitor.SetStop(false)
	monitor.SetPanicMode(false)
	start = time.Now()
	monitor.Delay()
	elapsed = time.Since(start)

	assert.True(t, elapsed >= 120*time.Millisecond, "Should delay at least the request time.")
	assert.True(t, elapsed <= 200*time.Millisecond, "Should not delay much longer than the requested time.")

	start = time.Now()
	monitor.SetPanicMode(true)
	monitor.Delay()
	elapsed = time.Since(start)

	assert.True(t, elapsed >= 45*time.Millisecond, "Should delay at least the request time.")
	assert.True(t, elapsed <= 100*time.Millisecond, "Should not delay much longer than the requested time.")
}

func TestBlueGreenStatusMonitorUpdateIpAddressFlags(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)
	hostInfo1, _ := host_info_util.NewHostInfoBuilder().SetHost("host1.example.com").SetPort(3306).Build()
	hostInfo2, _ := host_info_util.NewHostInfoBuilder().SetHost("host2.example.com").SetPort(3306).Build()
	statusCheckIntervalMap := map[driver_infrastructure.BlueGreenIntervalRate]int{
		driver_infrastructure.BASELINE: 300000,
	}
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()

	getMonitor := func() *bg.TestBlueGreenStatusMonitor {
		return bg.NewTestBlueGreenStatusMonitor(
			driver_infrastructure.SOURCE,
			"test-bg-id",
			hostInfo1,
			mockPluginService,
			emptyProps,
			statusCheckIntervalMap,
			nil,
		)
	}

	t.Run("CollectedTopologyTrue", func(t *testing.T) {
		monitor := getMonitor()
		monitor.SetCollectedTopology(true)
		monitor.SetAllStartTopologyIpChanged(true)
		monitor.SetAllStartTopologyEndpointsRemoved(true)
		monitor.SetAllTopologyChanged(true)

		monitor.UpdateIpAddressFlags()

		assert.False(t, monitor.GetAllStartTopologyIpChanged())
		assert.False(t, monitor.GetAllStartTopologyEndpointsRemoved())
		assert.False(t, monitor.GetAllTopologyChanged())
	})

	t.Run("CollectedTopologyFalseEmptyStartTopology", func(t *testing.T) {
		monitor := getMonitor()
		monitor.SetStartTopology([]*host_info_util.HostInfo{})
		monitor.SetCollectedTopology(false)
		monitor.UpdateIpAddressFlags()

		// Should be false when start topology is empty
		assert.False(t, monitor.GetAllStartTopologyIpChanged())
		assert.False(t, monitor.GetAllStartTopologyEndpointsRemoved())
		assert.False(t, monitor.GetAllTopologyChanged())
	})

	t.Run("CollectedTopologyFalseCollectedIpAddressesFalseNoIpChange", func(t *testing.T) {
		monitor := getMonitor()
		monitor.GetStartIpAddressesByHostMap().Put("host1.example.com", "192.168.1.1")
		monitor.GetStartIpAddressesByHostMap().Put("host2.example.com", "192.168.1.2")
		monitor.GetCurrentIpAddressesByHostMap().Put("host1.example.com", "192.168.1.1")
		monitor.GetCurrentIpAddressesByHostMap().Put("host2.example.com", "192.168.1.2")

		currentTopology := []*host_info_util.HostInfo{hostInfo1}
		monitor.SetStartTopology([]*host_info_util.HostInfo{hostInfo1, hostInfo2})
		monitor.SetCurrentTopology(&currentTopology)
		monitor.SetCollectedTopology(false)
		monitor.SetCollectedIpAddresses(false)

		monitor.UpdateIpAddressFlags()

		assert.False(t, monitor.GetAllStartTopologyIpChanged(), "IP addresses haven't changed, so AllStartTopologyIpChanged should be false")
		assert.False(t, monitor.GetAllStartTopologyEndpointsRemoved(), "Endpoints still have IP addresses, so AllStartTopologyEndpointsRemoved should be false")
		assert.False(t, monitor.GetAllTopologyChanged(), "Topology has changed but there is one host in common, so AllTopologyChanged should be false")
	})

	t.Run("CollectedTopologyFalseCollectedIpAddressesFalseAllIpChanged", func(t *testing.T) {
		monitor := getMonitor()
		currentTopology := []*host_info_util.HostInfo{hostInfo1, hostInfo2}
		monitor.SetStartTopology([]*host_info_util.HostInfo{hostInfo1, hostInfo2})
		monitor.SetCurrentTopology(&currentTopology)
		monitor.GetStartIpAddressesByHostMap().Put("host1.example.com", "192.168.1.1")
		monitor.GetStartIpAddressesByHostMap().Put("host2.example.com", "192.168.1.2")
		monitor.GetCurrentIpAddressesByHostMap().Put("host1.example.com", "192.168.2.1")
		monitor.GetCurrentIpAddressesByHostMap().Put("host2.example.com", "192.168.2.2")
		monitor.SetCollectedTopology(false)
		monitor.SetCollectedIpAddresses(false)

		monitor.UpdateIpAddressFlags()

		assert.True(t, monitor.GetAllStartTopologyIpChanged(), "All IP addresses have changed, so AllStartTopologyIpChanged should be true")
		assert.False(t, monitor.GetAllStartTopologyEndpointsRemoved(), "Endpoints still have IP addresses, so AllStartTopologyEndpointsRemoved should be false")
		assert.False(t, monitor.GetAllTopologyChanged(), "Topology hasn't changed (same hosts), so AllTopologyChanged should be false")
	})

	t.Run("CollectedTopologyFalseAllEndpointsRemoved", func(t *testing.T) {
		monitor := getMonitor()
		monitor.SetStartTopology([]*host_info_util.HostInfo{hostInfo1, hostInfo2})
		monitor.SetCurrentTopology(nil)
		monitor.GetStartIpAddressesByHostMap().Put("host1.example.com", "192.168.1.1")
		monitor.GetStartIpAddressesByHostMap().Put("host2.example.com", "192.168.1.2")
		monitor.GetCurrentIpAddressesByHostMap().Clear()
		monitor.SetCollectedTopology(false)
		monitor.SetCollectedIpAddresses(true)

		monitor.UpdateIpAddressFlags()

		assert.False(t, monitor.GetAllStartTopologyIpChanged(), "SetCollectedIpAddresses is true, should stay at initial value")
		assert.True(t, monitor.GetAllStartTopologyEndpointsRemoved(), "No more IP addresses, should mark as changed")
		assert.False(t, monitor.GetAllTopologyChanged(), "Current topology is empty, should mark as false")
	})

	t.Run("CollectedTopologyFalseAllTopologyChanged", func(t *testing.T) {
		monitor := getMonitor()
		currentTopology := []*host_info_util.HostInfo{hostInfo1}
		monitor.SetStartTopology([]*host_info_util.HostInfo{hostInfo2})
		monitor.SetCurrentTopology(&currentTopology)
		monitor.SetCollectedTopology(false)
		monitor.SetCollectedIpAddresses(true)

		monitor.UpdateIpAddressFlags()

		assert.False(t, monitor.GetAllStartTopologyIpChanged(), "Empty ip list, should be false")
		assert.False(t, monitor.GetAllStartTopologyEndpointsRemoved(), "Empty ip list, should be false")
		assert.True(t, monitor.GetAllTopologyChanged(), "Current topology is empty, should mark as false")
	})
}

func TestBlueGreenStatusMonitorCollectHostIpAddresses(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)
	hostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("test-host").SetPort(3306).Build()
	statusCheckIntervalMap := map[driver_infrastructure.BlueGreenIntervalRate]int{
		driver_infrastructure.BASELINE: 300000,
	}
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()

	monitor := bg.NewTestBlueGreenStatusMonitor(
		driver_infrastructure.SOURCE,
		"test-bg-id",
		hostInfo,
		mockPluginService,
		emptyProps,
		statusCheckIntervalMap,
		nil,
	)

	t.Run("EmptyHostNames", func(t *testing.T) {
		monitor.GetCurrentIpAddressesByHostMap().Put("host1.example.com", "192.168.1.1")
		assert.Equal(t, 1, monitor.GetCurrentIpAddressesByHostMap().Size())
		monitor.GetHostNames().Clear()
		assert.Equal(t, 0, monitor.GetHostNames().Size())

		monitor.CollectHostIpAddresses()

		assert.Equal(t, 0, monitor.GetCurrentIpAddressesByHostMap().Size())
	})

	t.Run("WithHostNamesCollectedIpAddressesFalse", func(t *testing.T) {
		monitor.GetHostNames().Put("localhost", true)

		monitor.GetCurrentIpAddressesByHostMap().Clear()
		monitor.GetStartIpAddressesByHostMap().Clear()
		monitor.SetCollectedIpAddresses(false)

		monitor.CollectHostIpAddresses()

		assert.Equal(t, 1, monitor.GetCurrentIpAddressesByHostMap().Size())

		localhostIp, exists := monitor.GetCurrentIpAddressesByHostMap().Get("localhost")
		assert.True(t, exists)
		assert.NotEmpty(t, localhostIp)

		assert.Equal(t, 0, monitor.GetStartIpAddressesByHostMap().Size())
	})

	t.Run("WithHostNamesCollectedIpAddressesTrue", func(t *testing.T) {
		monitor.GetHostNames().Clear()
		monitor.GetHostNames().Put("localhost", true)
		monitor.GetCurrentIpAddressesByHostMap().Clear()
		monitor.GetStartIpAddressesByHostMap().Clear()
		monitor.SetCollectedIpAddresses(true)

		monitor.CollectHostIpAddresses()

		assert.Equal(t, 1, monitor.GetCurrentIpAddressesByHostMap().Size())

		localhostIp, exists := monitor.GetCurrentIpAddressesByHostMap().Get("localhost")
		assert.True(t, exists)
		assert.NotEmpty(t, localhostIp)

		assert.Equal(t, 1, monitor.GetStartIpAddressesByHostMap().Size())
		localhostStartIp, exists := monitor.GetStartIpAddressesByHostMap().Get("localhost")
		assert.True(t, exists)
		assert.Equal(t, localhostIp, localhostStartIp)
	})
}

func TestBlueGreenStatusMonitorCollectTopology(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockConn.EXPECT().Close().Return(nil).AnyTimes()
	var mockDriverConn driver.Conn = mockConn
	hostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("test-host").SetPort(3306).Build()

	t.Run("NoHostListProvider", func(t *testing.T) {
		monitor, mockDriverDialect, _ := collectTopologySetUp(hostInfo, ctrl)
		mockDriverDialect.EXPECT().IsClosed(gomock.Any()).Return(true).AnyTimes()
		err := monitor.CollectTopology()
		assert.NoError(t, err, "Should not error when no host list provider is set.")
	})

	t.Run("NoConn", func(t *testing.T) {
		monitor, mockDriverDialect, mockHostListProvider := collectTopologySetUp(hostInfo, ctrl)
		mockDriverDialect.EXPECT().IsClosed(gomock.Any()).Return(true).AnyTimes()
		monitor.SetHostListProvider(mockHostListProvider)
		err := monitor.CollectTopology()
		assert.NoError(t, err, "Should not error when no connection is set.")
	})

	t.Run("ConnClosed", func(t *testing.T) {
		monitor, mockDriverDialect, mockHostListProvider := collectTopologySetUp(hostInfo, ctrl)
		monitor.SetHostListProvider(mockHostListProvider)
		monitor.SetConnection(&mockDriverConn)
		mockDriverDialect.EXPECT().IsClosed(gomock.Any()).Return(true).AnyTimes()
		err := monitor.CollectTopology()
		assert.NoError(t, err, "Should not error when connection is closed.")
	})

	t.Run("ForceRefreshError", func(t *testing.T) {
		monitor, mockDriverDialect, mockHostListProvider := collectTopologySetUp(hostInfo, ctrl)
		monitor.SetHostListProvider(mockHostListProvider)
		monitor.SetConnection(&mockDriverConn)
		mockDriverDialect.EXPECT().IsClosed(gomock.Any()).Return(false).AnyTimes()
		mockHostListProvider.EXPECT().ForceRefresh(gomock.Any()).Return(nil, errors.New("test-error"))
		err := monitor.CollectTopology()
		assert.Error(t, err, "Should error when ForceRefresh fails.")
	})

	t.Run("ForceRefreshSuccessNoCollection", func(t *testing.T) {
		monitor, mockDriverDialect, mockHostListProvider := collectTopologySetUp(hostInfo, ctrl)
		monitor.SetHostListProvider(mockHostListProvider)
		monitor.SetConnection(&mockDriverConn)
		mockDriverDialect.EXPECT().IsClosed(gomock.Any()).Return(false).AnyTimes()
		mockHostListProvider.EXPECT().ForceRefresh(gomock.Any()).Return([]*host_info_util.HostInfo{hostInfo}, nil)
		_, hostInfoInHostNames := monitor.GetHostNames().Get(hostInfo.GetHost())
		assert.False(t, hostInfoInHostNames)
		err := monitor.CollectTopology()
		assert.NoError(t, err, "Should not error when ForceRefresh returns hosts.")
		_, hostInfoInHostNames = monitor.GetHostNames().Get(hostInfo.GetHost())
		assert.False(t, hostInfoInHostNames)
	})

	t.Run("ForceRefreshSuccessCollection", func(t *testing.T) {
		monitor, mockDriverDialect, mockHostListProvider := collectTopologySetUp(hostInfo, ctrl)
		monitor.SetHostListProvider(mockHostListProvider)
		monitor.SetConnection(&mockDriverConn)
		monitor.SetCollectedTopology(true)
		mockDriverDialect.EXPECT().IsClosed(gomock.Any()).Return(false).AnyTimes()
		mockHostListProvider.EXPECT().ForceRefresh(gomock.Any()).Return([]*host_info_util.HostInfo{hostInfo}, nil)
		_, hostInfoInHostNames := monitor.GetHostNames().Get(hostInfo.GetHost())
		assert.False(t, hostInfoInHostNames)
		err := monitor.CollectTopology()
		assert.NoError(t, err, "Should not error when ForceRefresh returns hosts.")
		_, hostInfoInHostNames = monitor.GetHostNames().Get(hostInfo.GetHost())
		assert.True(t, hostInfoInHostNames)
	})
}

func collectTopologySetUp(hostInfo *host_info_util.HostInfo, ctrl *gomock.Controller) (*bg.TestBlueGreenStatusMonitor,
	*mock_driver_infrastructure.MockDriverDialect, *mock_driver_infrastructure.MockHostListProvider) {
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDriverDialect := mock_driver_infrastructure.NewMockDriverDialect(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)
	mockHostListProvider := mock_driver_infrastructure.NewMockHostListProvider(ctrl)

	statusCheckIntervalMap := map[driver_infrastructure.BlueGreenIntervalRate]int{
		driver_infrastructure.BASELINE: 300000,
	}

	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().GetTargetDriverDialect().Return(mockDriverDialect).AnyTimes()
	mockPluginService.EXPECT().CreateHostListProvider(gomock.Any()).Return(mockHostListProvider).AnyTimes()

	monitor := bg.NewTestBlueGreenStatusMonitor(
		driver_infrastructure.SOURCE,
		"test-bg-id",
		hostInfo,
		mockPluginService,
		emptyProps,
		statusCheckIntervalMap,
		nil,
	)
	return monitor, mockDriverDialect, mockHostListProvider
}

func TestBlueGreenStatusMonitorInitHostListProvider(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)
	hostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("test-host").SetPort(3306).Build()
	statusCheckIntervalMap := map[driver_infrastructure.BlueGreenIntervalRate]int{
		driver_infrastructure.BASELINE: 300000,
	}
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().CreateHostListProvider(gomock.Any()).Return(&driver_infrastructure.RdsHostListProvider{}).AnyTimes()

	monitor := bg.NewTestBlueGreenStatusMonitor(
		driver_infrastructure.SOURCE,
		"test-bg-id",
		hostInfo,
		mockPluginService,
		emptyProps,
		statusCheckIntervalMap,
		nil,
	)
	monitor.SetConnectionHostInfoCorrect(false)
	assert.Nil(t, monitor.GetHostListProvider(), "HostListProvider should be nil upon monitor creation.")

	monitor.InitHostListProvider()
	assert.Nil(t, monitor.GetHostListProvider(), "If ConnectionHostInfo is incorrect, will not initialize")

	monitor.SetConnectionHostInfoCorrect(true)
	monitor.InitHostListProvider()
	hostListProvider := monitor.GetHostListProvider()
	assert.NotNil(t, hostListProvider, "Should initialize when hostInfo is correct")

	monitor.InitHostListProvider()
	assert.NotNil(t, monitor.GetHostListProvider(), "Should stay initialized as the same value")
	assert.Equal(t, hostListProvider, monitor.GetHostListProvider())

	monitor.SetConnectionHostInfo(hostInfo)
	monitor.SetHostListProvider(nil)
	monitor.InitHostListProvider()
	assert.NotNil(t, monitor.GetHostListProvider(), "Should initialize with additional values from hostInfo")
}

func TestBlueGreenStatusMonitorOpenConnection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)
	mockDriverDialect := mock_driver_infrastructure.NewMockDriverDialect(ctrl)
	mockConn := mock_database_sql_driver.NewMockConn(ctrl)

	hostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("test-host").SetPort(3306).Build()

	statusCheckIntervalMap := map[driver_infrastructure.BlueGreenIntervalRate]int{
		driver_infrastructure.BASELINE: 300000,
	}

	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().GetTargetDriverDialect().Return(mockDriverDialect).AnyTimes()
	mockDriverDialect.EXPECT().IsClosed(gomock.Any()).Return(false).AnyTimes()

	monitor := bg.NewTestBlueGreenStatusMonitor(
		driver_infrastructure.SOURCE,
		"test-bg-id",
		hostInfo,
		mockPluginService,
		emptyProps,
		statusCheckIntervalMap,
		nil,
	)

	t.Run("InitialHostInfoSuccess", func(t *testing.T) {
		mockPluginService.EXPECT().ForceConnect(gomock.Any(), gomock.Any()).Return(mockConn, nil)
		monitor.OpenConnection()

		conn := monitor.GetConnection()
		assert.NotNil(t, conn)
		assert.False(t, monitor.GetPanicMode())
	})

	t.Run("InitialHostInfoFailure", func(t *testing.T) {
		monitor.SetConnectedIpAddress("localhost")
		monitor.SetConnection(nil)
		mockPluginService.EXPECT().ForceConnect(gomock.Any(), gomock.Any()).Return(nil, errors.New("test-error"))
		monitor.OpenConnection()

		conn := monitor.GetConnection()
		assert.Nil(t, conn)
		assert.True(t, monitor.GetPanicMode())
	})

	t.Run("IpAddress", func(t *testing.T) {
		monitor.SetConnectedIpAddress("localhost")
		monitor.SetUseIpAddress(true)
		mockPluginService.EXPECT().ForceConnect(gomock.Any(), gomock.Any()).Return(mockConn, nil)
		monitor.OpenConnection()

		conn := monitor.GetConnection()
		assert.NotNil(t, conn)
		assert.False(t, monitor.GetPanicMode())
	})
}

func TestBlueGreenStatusMonitorCloseConnection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)
	mockDriverDialect := mock_driver_infrastructure.NewMockDriverDialect(ctrl)
	hostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("test-host").SetPort(3306).Build()
	mockDriverDialect.EXPECT().IsClosed(gomock.Any()).Return(false).AnyTimes()
	statusCheckIntervalMap := map[driver_infrastructure.BlueGreenIntervalRate]int{
		driver_infrastructure.BASELINE: 300000,
	}
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().GetTargetDriverDialect().Return(mockDriverDialect).AnyTimes()

	monitor := bg.NewTestBlueGreenStatusMonitor(
		driver_infrastructure.SOURCE,
		"test-bg-id",
		hostInfo,
		mockPluginService,
		emptyProps,
		statusCheckIntervalMap,
		nil,
	)

	// Set up a mock connection first
	var mockConn driver.Conn = &MockConn{}
	monitor.SetConnection(&mockConn)

	conn := monitor.GetConnection()
	assert.NotNil(t, conn, "connection should be set to mockConn..")

	monitor.CloseConnection()

	conn = monitor.GetConnection()
	assert.Nil(t, conn, "connection should be nil after being closed.")
}

func TestBlueGreenStatusMonitorResetCollectedData(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)

	hostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("test-host").SetPort(3306).Build()

	statusCheckIntervalMap := map[driver_infrastructure.BlueGreenIntervalRate]int{
		driver_infrastructure.BASELINE: 300000,
	}

	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()

	monitor := bg.NewTestBlueGreenStatusMonitor(
		driver_infrastructure.SOURCE,
		"test-bg-id",
		hostInfo,
		mockPluginService,
		emptyProps,
		statusCheckIntervalMap,
		nil,
	)

	monitor.GetHostNames().Put("test-host", true)
	monitor.GetHostNames().Put("test-host-2", true)
	assert.Equal(t, 2, monitor.GetHostNames().Size(), "Data should be added to HostNames.")

	monitor.ResetCollectedData()
	assert.Equal(t, 0, monitor.GetHostNames().Size(), "HostNames should be cleared after reset.")
}

func TestBlueGreenStatusMonitorCollectStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)
	hostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("test-host").SetPort(3306).Build()
	statusCheckIntervalMap := map[driver_infrastructure.BlueGreenIntervalRate]int{
		driver_infrastructure.BASELINE: 300000,
	}
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()

	monitor := bg.NewTestBlueGreenStatusMonitor(
		driver_infrastructure.TARGET,
		"test-bg-id",
		hostInfo,
		mockPluginService,
		emptyProps,
		statusCheckIntervalMap,
		nil,
	)

	t.Run("NoConnection", func(t *testing.T) {
		monitor.CollectStatus()
		assert.True(t, monitor.GetCurrentPhase().Equals(driver_infrastructure.NOT_CREATED))
		assert.True(t, monitor.GetPanicMode())
	})

	t.Run("WithOpenConnectionStatusUnavailable", func(t *testing.T) {
		mockDriverDialect := mock_driver_infrastructure.NewMockDriverDialect(ctrl)
		mockPluginService.EXPECT().GetTargetDriverDialect().Return(mockDriverDialect).Times(2)
		mockDriverDialect.EXPECT().IsClosed(gomock.Any()).Return(false).Times(2)
		mockDialect.EXPECT().IsBlueGreenStatusAvailable(gomock.Any()).Return(false)

		var mockConn driver.Conn = &MockConn{}
		monitor.SetConnection(&mockConn)

		monitor.CollectStatus()

		assert.Equal(t, driver_infrastructure.NOT_CREATED, monitor.GetCurrentPhase(),
			"Unavailable status should return NOT_CREATED phase")
		assert.True(t, monitor.GetPanicMode())
	})

	t.Run("WithOpenThenClosedConnectionStatusUnavailable", func(t *testing.T) {
		mockDriverDialect := mock_driver_infrastructure.NewMockDriverDialect(ctrl)
		mockPluginService.EXPECT().GetTargetDriverDialect().Return(mockDriverDialect).Times(2)
		mockDriverDialect.EXPECT().IsClosed(gomock.Any()).Return(false).Times(1)
		mockDriverDialect.EXPECT().IsClosed(gomock.Any()).Return(true).Times(1)
		mockDialect.EXPECT().IsBlueGreenStatusAvailable(gomock.Any()).Return(false)

		var mockConn driver.Conn = &MockConn{}
		monitor.SetConnection(&mockConn)

		monitor.CollectStatus()

		assert.True(t, monitor.GetCurrentPhase().IsZero(),
			"When connection closes unexpectedly, phase should be 0")
		assert.True(t, monitor.GetPanicMode())
	})

	t.Run("StatusAvailableNilStatusInfo", func(t *testing.T) {
		monitor.SetCollectedTopology(true)
		mockDriverDialect := mock_driver_infrastructure.NewMockDriverDialect(ctrl)
		mockPluginService.EXPECT().GetTargetDriverDialect().Return(mockDriverDialect).Times(1)
		mockDriverDialect.EXPECT().IsClosed(gomock.Any()).Return(false).Times(1)
		mockDialect.EXPECT().IsBlueGreenStatusAvailable(gomock.Any()).Return(true)
		mockDialect.EXPECT().GetBlueGreenStatus(gomock.Any()).Return([]driver_infrastructure.BlueGreenResult{
			{
				Version:  "1.0",
				Endpoint: "prod-aurora-cluster.cluster-abc123def456.us-east-1.rds.amazonaws.com",
				Port:     5432,
				Role:     "BLUE_GREEN_DEPLOYMENT_SOURCE",
				Status:   "AVAILABLE",
			},
		}, nil)
		var mockConn driver.Conn = &MockConn{}
		monitor.SetConnection(&mockConn)

		monitor.CollectStatus()

		assert.True(t, monitor.GetCurrentPhase().IsZero(),
			"Phase should be 0 when there is no matching status")
		assert.True(t, monitor.GetPanicMode())
	})

	t.Run("StatusAvailable", func(t *testing.T) {
		monitor.SetCollectedTopology(true)
		mockDriverDialect := mock_driver_infrastructure.NewMockDriverDialect(ctrl)
		mockPluginService.EXPECT().GetTargetDriverDialect().Return(mockDriverDialect).Times(1)
		mockDriverDialect.EXPECT().IsClosed(gomock.Any()).Return(false).Times(1)
		mockDialect.EXPECT().IsBlueGreenStatusAvailable(gomock.Any()).Return(true)
		mockPluginService.EXPECT().CreateHostListProvider(gomock.Any()).Return(&driver_infrastructure.RdsHostListProvider{})
		mockDialect.EXPECT().GetBlueGreenStatus(gomock.Any()).Return([]driver_infrastructure.BlueGreenResult{
			{
				Version:  "1.0",
				Endpoint: "prod-aurora-cluster.cluster-abc123def456.us-east-1.rds.amazonaws.com",
				Port:     5432,
				Role:     "BLUE_GREEN_DEPLOYMENT_SOURCE",
				Status:   "AVAILABLE",
			},
			{
				Version:  "1.1",
				Endpoint: "prod-aurora-cluster-target.cluster-xyz789def456.us-east-1.rds.amazonaws.com",
				Port:     5432,
				Role:     "BLUE_GREEN_DEPLOYMENT_TARGET",
				Status:   "SWITCHOVER_IN_PROGRESS",
			},
		}, nil)
		var mockConn driver.Conn = &MockConn{}
		monitor.SetConnection(&mockConn)

		monitor.CollectStatus()

		assert.Equal(t, driver_infrastructure.IN_PROGRESS, monitor.GetCurrentPhase(),
			"Status should match the BlueGreenResult for TARGET")
		assert.False(t, monitor.GetPanicMode())
	})
}
