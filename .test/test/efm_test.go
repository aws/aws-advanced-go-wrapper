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
	"strings"
	"testing"
	"time"
	"weak"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	awssql "github.com/aws/aws-advanced-go-wrapper/awssql/driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/efm"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/services"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	pgx_driver "github.com/aws/aws-advanced-go-wrapper/pgx-driver"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

// efmTestContainer creates a FullServicesContainer with a real MonitorManager for EFM tests.
func efmTestContainer(props *utils.RWMap[string, string]) (*services.FullServicesContainer, driver_infrastructure.PluginService) {
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	monitorManager := services.NewMonitorManager(5*time.Minute, nil)
	container := &services.FullServicesContainer{
		Telemetry: telemetryFactory,
		Monitor:   monitorManager,
	}
	mockTargetDriver := &MockTargetDriver{}
	pluginManager := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, container, props)
	container.SetPluginManager(pluginManager)
	pluginService, _ := plugin_helpers.NewPluginServiceImpl(container, pgx_driver.NewPgxDriverDialect(), props, pgTestDsn)
	container.SetPluginService(pluginService)
	return container, pluginService
}

// efmTestContainerWithMockMonitor creates a container with a gomock MockMonitorService for unit tests.
func efmTestContainerWithMockMonitor(ctrl *gomock.Controller, props *utils.RWMap[string, string]) (
	*services.FullServicesContainer, driver_infrastructure.PluginService, *mock_driver_infrastructure.MockMonitorService,
) {
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	mockMonitorService := mock_driver_infrastructure.NewMockMonitorService(ctrl)
	container := &services.FullServicesContainer{
		Telemetry: telemetryFactory,
		Monitor:   mockMonitorService,
	}
	mockTargetDriver := &MockTargetDriver{}
	pluginManager := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, container, props)
	container.SetPluginManager(pluginManager)
	pluginService, _ := plugin_helpers.NewPluginServiceImpl(container, pgx_driver.NewPgxDriverDialect(), props, pgTestDsn)
	container.SetPluginService(pluginService)
	return container, pluginService, mockMonitorService
}

func TestMonitorConnectionState(t *testing.T) {
	var conn driver.Conn = &MockDriverConn{}
	state := efm.NewMonitorConnectionState(&conn)

	assert.NotNil(t, state.GetConn())
	assert.True(t, state.IsActive())
	// By default, hostUnhealthy is false. When host is healthy should not abort.
	assert.False(t, state.ShouldAbort())

	state.SetHostUnhealthy(true)
	// If there is an active unhealthy conn, should abort.
	assert.True(t, state.ShouldAbort())

	state.SetInactive()
	assert.Nil(t, state.GetConn())
	assert.False(t, state.IsActive())
	// If the unhealthy conn is inactive, no need to abort.
	assert.False(t, state.ShouldAbort())

	state.SetHostUnhealthy(false)
	// Not unhealthy and no conn, no need to abort.
	assert.False(t, state.ShouldAbort())
}

func TestHostMonitoringServiceImpl(t *testing.T) {
	propsMap := map[string]string{
		property_util.DRIVER_PROTOCOL.Name:               "postgresql",
		property_util.FAILURE_DETECTION_TIME_MS.Name:     "0",
		property_util.FAILURE_DETECTION_INTERVAL_MS.Name: "900",
		property_util.FAILURE_DETECTION_COUNT.Name:       "3",
		property_util.MONITOR_DISPOSAL_TIME_MS.Name:      "600000",
	}
	props := utils.NewRWMapFromMap(propsMap)
	container, _ := efmTestContainer(props)
	monitorService, err := efm.NewHostMonitoringServiceImpl(container, props)
	assert.Nil(t, err)
	var testConn driver.Conn = &MockConn{throwError: false}

	_, err = monitorService.StartMonitoring(nil, nil, emptyProps)
	// Monitoring with an invalid conn should fail.
	assert.True(t, strings.Contains(err.Error(), "conn"))

	_, err = monitorService.StartMonitoring(&testConn, nil, emptyProps)
	// Monitoring with an invalid monitoring HostInfo should fail.
	assert.True(t, strings.Contains(err.Error(), "hostInfo"))

	// Monitoring with correct parameters should create a new monitor.
	state, err := monitorService.StartMonitoring(&testConn, mockHostInfo, props)
	assert.Nil(t, err)
	assert.True(t, state.IsActive())

	state2, err := monitorService.StartMonitoring(&testConn, mockHostInfo, props)
	assert.Nil(t, err)
	assert.True(t, state2.IsActive())

	monitorService.StopMonitoring(state2, testConn)
	// States are not tied together. First state remains active as one is cancelled.
	assert.False(t, state2.IsActive())
	assert.True(t, state.IsActive())

	monitorService.StopMonitoring(state, testConn)
	assert.False(t, state.IsActive())
}

func TestHostMonitoringPluginFactory(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	factory := efm.HostMonitoringPluginFactory{}

	// Plugin factory should not return an instance when container has nil pluginService.
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(nil)
	_, err := factory.GetInstance(mockContainer, nil)
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "pluginService"))

	// Plugin factory should not return an instance when props is nil.
	props := MakeMapFromKeysAndVals(property_util.HOST.Name, "host")
	container, _, _ := efmTestContainerWithMockMonitor(ctrl, props)
	_, err = factory.GetInstance(container, nil)
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "properties"))

	// Plugin factory should return an instance given valid parameters.
	_, err = factory.GetInstance(container, props)
	assert.Nil(t, err)
}

func mockHostMonitoringPlugin(props *utils.RWMap[string, string]) (*efm.HostMonitorConnectionPlugin, error) {
	factory := efm.HostMonitoringPluginFactory{}
	if props == nil || props.Size() == 0 {
		props = MakeMapFromKeysAndVals(
			property_util.USER.Name, "user",
			property_util.PASSWORD.Name, "password",
			property_util.PORT.Name, "5432",
			property_util.HOST.Name, "host",
			property_util.DATABASE.Name, "dbName",
			property_util.PLUGINS.Name, "test",
			property_util.DRIVER_PROTOCOL.Name, "postgresql",
		)
	}

	container, pluginService := efmTestContainer(props)
	_ = container
	plugin, err := factory.GetInstance(container, props)
	if err != nil {
		return nil, err
	}
	err = pluginService.SetCurrentConnection(&MockDriverConnection{}, mockHostInfo, plugin)
	monitoringPlugin, _ := plugin.(*efm.HostMonitorConnectionPlugin)

	// Reset caches and query counter.
	awssql.ClearCaches()
	queryCounter = 0
	return monitoringPlugin, err
}

var queryCounter = 0

func incrementQueryCounter() (any, any, bool, error) {
	queryCounter++
	return nil, nil, false, nil
}

func TestHostMonitoringPluginConnect(t *testing.T) {
	plugin, err := mockHostMonitoringPlugin(emptyProps)
	assert.Nil(t, err)
	rdsHostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("instance-a-1.xyz.us-east-2.rds.amazonaws.com").Build()
	assert.Nil(t, err)
	connectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		return &MockDriverConnection{}, nil
	}

	assert.True(t, utils.IsRdsDns(rdsHostInfo.Host))
	rdsHostInfo.ResetAliases()
	aliasToBeRemoved := "old-alias"
	rdsHostInfo.AddAlias(aliasToBeRemoved)

	conn, err := plugin.Connect(rdsHostInfo, emptyProps, true, connectFunc)
	assert.Nil(t, err)
	assert.NotNil(t, conn)
	// Connect should update the aliases of a given Rds HostInfo.
	_, isAlias := rdsHostInfo.AllAliases[aliasToBeRemoved]
	assert.False(t, isAlias)
}

func TestHostMonitoringPluginNotifyConnectionChanged(t *testing.T) {
	plugin, err := mockHostMonitoringPlugin(emptyProps)
	assert.Nil(t, err)
	// Set monitoring HostInfo by executing a network bound method.
	_, _, _, err = plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, incrementQueryCounter)
	assert.Nil(t, err)
	assert.NotNil(t, plugin.GetMonitoringHostInfo())

	// Monitoring HostInfo should be reset when host changes.
	hostChanged := map[driver_infrastructure.HostChangeOptions]bool{driver_infrastructure.HOST_CHANGED: true}
	action := plugin.NotifyConnectionChanged(hostChanged)
	assert.Nil(t, plugin.GetMonitoringHostInfo())
	assert.Equal(t, driver_infrastructure.NO_OPINION, action)

	plugin, err = mockHostMonitoringPlugin(emptyProps)
	assert.Nil(t, err)
	// Set monitoring HostInfo by executing a network bound method.
	_, _, _, err = plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, incrementQueryCounter)
	assert.Nil(t, err)
	assert.NotNil(t, plugin.GetMonitoringHostInfo())

	// Monitoring HostInfo should be reset when hostname changes.
	hostNameChanged := map[driver_infrastructure.HostChangeOptions]bool{driver_infrastructure.HOSTNAME: true}
	action = plugin.NotifyConnectionChanged(hostNameChanged)
	assert.Nil(t, plugin.GetMonitoringHostInfo())
	assert.Equal(t, driver_infrastructure.NO_OPINION, action)
}

func TestHostMonitoringPluginExecuteMonitoringUnnecessary(t *testing.T) {
	plugin, err := mockHostMonitoringPlugin(emptyProps)
	assert.Nil(t, err)
	assert.Zero(t, queryCounter)

	_, _, _, err = plugin.Execute(nil, utils.CONN_CLOSE, incrementQueryCounter)
	assert.Nil(t, err)

	// When method to be executed is not network bound, no monitoring occurs.
	assert.Equal(t, 1, queryCounter)
}

func TestHostMonitoringPluginExecuteMonitoringEnabled(t *testing.T) {
	plugin, err := mockHostMonitoringPlugin(emptyProps)
	assert.Nil(t, err)
	assert.Zero(t, queryCounter)

	_, _, _, err = plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, incrementQueryCounter)
	assert.Nil(t, err)
	assert.Equal(t, 1, queryCounter)
}

func TestHostMonitoringPluginExecuteThrowsError(t *testing.T) {
	factory := efm.HostMonitoringPluginFactory{}
	// Use valid props for container creation, but the plugin service won't have a current host info set.
	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.HOST.Name, "host",
	)
	container, _ := efmTestContainer(props)
	plugin, _ := factory.GetInstance(container, props)
	// Reset caches and query counter.
	awssql.ClearCaches()
	queryCounter = 0

	assert.Zero(t, queryCounter)

	_, _, _, err := plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, incrementQueryCounter)
	// Plugin service unable to supply a host info to monitor (no current connection set).
	assert.NotNil(t, err)
	assert.Zero(t, queryCounter)
}

func TestMonitorCanDispose(t *testing.T) {
	props := emptyProps
	container, _ := efmTestContainer(props)
	monitor := efm.NewHostMonitorImpl(container, mockHostInfo, emptyProps, 0, 10, 0, telemetry.NilTelemetryCounter{})
	monitor.MonitoringConn = &MockDriverConnection{}
	var conn driver.Conn = &MockDriverConn{}
	state := efm.NewMonitorConnectionState(&conn)

	// If there are no states, monitor can be disposed.
	assert.True(t, monitor.CanDispose())

	monitor.NewStates.Put(time.Time{}, nil)
	assert.False(t, monitor.CanDispose())

	monitor.ActiveStates.Enqueue(weak.Make(state))
	assert.False(t, monitor.CanDispose())

	monitor.NewStates.Clear()
	assert.False(t, monitor.CanDispose())
	monitor.Close()
}

func TestMonitorClose(t *testing.T) {
	props := emptyProps
	container, _ := efmTestContainer(props)
	monitor := efm.NewHostMonitorImpl(container, mockHostInfo, emptyProps, 0, 10, 0, telemetry.NilTelemetryCounter{})
	mockConn := &MockDriverConnection{}
	monitor.MonitoringConn = mockConn

	assert.False(t, monitor.Stopped.Load())
	assert.False(t, mockConn.IsClosed)
	monitor.Close()
	assert.True(t, mockConn.IsClosed)
}

func TestMonitorCheckConnectionStatusOpenConnection(t *testing.T) {
	pluginService, _, _, container, _ := beforePluginServiceTests()
	container.SetPluginService(pluginService)
	container.Monitor = services.NewMonitorManager(5*time.Minute, nil)
	monitor := efm.NewHostMonitorImpl(container, mockHostInfo, emptyProps, 0, 10, 0, telemetry.NilTelemetryCounter{})
	monitor.MonitoringConn = &MockConn{isInvalid: true}

	assert.True(t, monitor.CheckConnectionStatus())
	monitor.Close()
}

func TestMonitorCheckConnectionStatusOpenConnectionFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(emptyProps)
	mockPluginService.EXPECT().GetTelemetryFactory().Return(telemetryFactory).AnyTimes()
	mockPluginService.EXPECT().GetTelemetryContext().Return(nil).AnyTimes()
	mockPluginService.EXPECT().SetTelemetryContext(gomock.Any()).AnyTimes()
	mockPluginService.EXPECT().GetTargetDriverDialect().Return(pgx_driver.NewPgxDriverDialect()).AnyTimes()
	mockPluginService.EXPECT().ForceConnect(gomock.Any(), gomock.Any()).Return(nil, errors.New("connection failed"))

	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()
	mockContainer.EXPECT().GetEventPublisher().Return(nil).AnyTimes()

	monitor := efm.NewHostMonitorImpl(mockContainer, mockHostInfo, emptyProps, 0, 10, 0, telemetry.NilTelemetryCounter{})
	monitor.MonitoringConn = &MockConn{isInvalid: true}
	assert.False(t, monitor.CheckConnectionStatus())
}

func TestMonitorCheckConnectionStatusIsReachable(t *testing.T) {
	props := MakeMapFromKeysAndVals(property_util.DRIVER_PROTOCOL.Name, "postgresql")
	container, _ := efmTestContainer(props)
	monitor := efm.NewHostMonitorImpl(container, mockHostInfo, emptyProps, 0, 10, 0, telemetry.NilTelemetryCounter{})

	monitor.MonitoringConn = &MockConn{throwError: false}
	assert.True(t, monitor.CheckConnectionStatus())

	monitor.MonitoringConn = &MockConn{throwError: true}
	assert.False(t, monitor.CheckConnectionStatus())
	monitor.Close()
}

func TestMonitorCheckConnectionStatusNewConn(t *testing.T) {
	pluginService, _, _, container, _ := beforePluginServiceTests()
	container.SetPluginService(pluginService)
	container.Monitor = services.NewMonitorManager(5*time.Minute, nil)
	monitor := efm.NewHostMonitorImpl(container, mockHostInfo, emptyProps, 0, 10, 0, telemetry.NilTelemetryCounter{})

	assert.True(t, monitor.CheckConnectionStatus())
	monitor.Close()
}

func TestMonitorNewConnWithMonitoringProperties(t *testing.T) {
	_, mockPluginManager, _, _, _ := beforePluginServiceTests()
	props := MakeMapFromKeysAndVals(
		"host", "host",
		"port", "1234",
		"user", "user",
		"password", "password",
		"monitoring-user", "monitor-user",
		"monitoring-password", "monitor-password",
	)
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	monitorManager := services.NewMonitorManager(5*time.Minute, nil)
	container := &services.FullServicesContainer{
		Telemetry: telemetryFactory,
		Monitor:   monitorManager,
	}
	mockTargetDriver := &MockTargetDriver{}
	realPluginManager := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, container, props)
	mockPluginManager = &MockPluginManager{realPluginManager, nil, nil}
	container.SetPluginManager(mockPluginManager)
	pluginService, _ := plugin_helpers.NewPluginServiceImpl(container, pgx_driver.NewPgxDriverDialect(), props, pgTestDsn)
	container.SetPluginService(pluginService)

	monitor := efm.NewHostMonitorImpl(container, mockHostInfo, props, 0, 10, 0, telemetry.NilTelemetryCounter{})
	monitor.Close() // Ensures none of the monitoring goroutines are running in the background.

	assert.True(t, monitor.CheckConnectionStatus())
	assert.NotNil(t, mockPluginManager.ForceConnectProps)
	assert.Equal(t, mockPluginManager.ForceConnectProps.Size(), 4)
	assert.Equal(t, property_util.HOST.Get(mockPluginManager.ForceConnectProps), "host")
	assert.Equal(t, property_util.PORT.Get(mockPluginManager.ForceConnectProps), "1234")
	assert.Equal(t, property_util.USER.Get(mockPluginManager.ForceConnectProps), "monitor-user")
	assert.Equal(t, property_util.PASSWORD.Get(mockPluginManager.ForceConnectProps), "monitor-password")
}

func TestMonitorUpdateHostHealthStatusValid(t *testing.T) {
	props := emptyProps
	container, _ := efmTestContainer(props)
	monitor := efm.NewHostMonitorImpl(container, mockHostInfo, emptyProps, 0, 10, 0, telemetry.NilTelemetryCounter{})
	monitor.Close() // Ensures none of the monitoring goroutines are running in the background.

	monitor.FailureCount.Store(1)
	monitor.InvalidHostStartTime = time.Now()
	monitor.HostUnhealthy.Store(true)

	monitor.UpdateHostHealthStatus(true, time.Now(), time.Now().Add(5))

	assert.Zero(t, monitor.FailureCount.Load())
	assert.Zero(t, monitor.InvalidHostStartTime)
	assert.False(t, monitor.HostUnhealthy.Load())
	monitor.Close()
}

func TestMonitorUpdateHostHealthStatusInvalid(t *testing.T) {
	props := emptyProps
	container, _ := efmTestContainer(props)
	monitor := efm.NewHostMonitorImpl(container, mockHostInfo, emptyProps, 0, 10, 0, telemetry.NilTelemetryCounter{})
	monitor.Close() // Ensures none of the monitoring goroutines are running in the background.

	assert.Zero(t, monitor.FailureCount.Load())
	assert.Zero(t, monitor.InvalidHostStartTime)
	assert.False(t, monitor.HostUnhealthy.Load())

	startTime := time.Now()
	monitor.UpdateHostHealthStatus(false, startTime, time.Now().Add(5))

	assert.Equal(t, int32(1), monitor.FailureCount.Load())
	assert.Equal(t, startTime, monitor.InvalidHostStartTime)
	assert.True(t, monitor.HostUnhealthy.Load())
	monitor.Close()
}
