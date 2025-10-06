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
	"strings"
	"testing"
	"time"
	"weak"

	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"

	awssql "github.com/aws/aws-advanced-go-wrapper/awssql/driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/efm"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"

	"github.com/stretchr/testify/assert"
)

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

func TestMonitorServiceImpl(t *testing.T) {
	if efm.EFM_MONITORS != nil {
		efm.EFM_MONITORS.Clear()
		efm.EFM_MONITORS = nil
	}
	assert.Nil(t, efm.EFM_MONITORS)

	_, pluginService := initializeTest(map[string]string{property_util.DRIVER_PROTOCOL.Name: "mysql"}, true, false, false, false, false, false)
	monitorService, _ := efm.NewMonitorServiceImpl(pluginService)
	var testConn driver.Conn = &MockConn{throwError: false}

	assert.NotNil(t, efm.EFM_MONITORS)
	assert.Zero(t, efm.EFM_MONITORS.Size())

	_, err := monitorService.StartMonitoring(nil, nil, emptyProps, 0, 0, 0, 0)
	// Monitoring with an invalid conn should fail.
	assert.True(t, strings.Contains(err.Error(), "conn"))

	_, err = monitorService.StartMonitoring(&testConn, nil, emptyProps, 0, 0, 0, 0)
	// Monitoring with an invalid monitoring HostInfo should fail.
	assert.True(t, strings.Contains(err.Error(), "hostInfo"))

	// Monitoring with correct parameters should create a new monitor.
	state, err := monitorService.StartMonitoring(&testConn, mockHostInfo, emptyProps, 0, 900, 3, 600000)
	monitorKey := fmt.Sprintf("%d:%d:%d:%s", 0, 900, 3, mockHostInfo.GetUrl())

	assert.Nil(t, err)
	assert.True(t, state.IsActive())
	assert.Equal(t, efm.EFM_MONITORS.Size(), 1)
	val, ok := efm.EFM_MONITORS.Get(monitorKey, time.Minute)
	assert.True(t, ok)
	assert.NotNil(t, val)
	monitor, ok := val.(*efm.MonitorImpl)
	assert.True(t, ok)

	state2, err := monitorService.StartMonitoring(&testConn, mockHostInfo, emptyProps, 0, 900, 3, 600000)
	assert.Nil(t, err)
	// Monitoring on the same host should not increase the cache size.
	assert.Equal(t, efm.EFM_MONITORS.Size(), 1)
	assert.True(t, state2.IsActive())

	monitor.MonitoringConn = testConn
	monitoringConn := &MockConn{}
	monitor.MonitoringConn = monitoringConn

	// Let the newStates monitoring routine update.
	for monitor.ActiveStates.Size() != 2 {
		time.Sleep(time.Second)
	}
	assert.Equal(t, 2, monitor.ActiveStates.Size())
	assert.Equal(t, 0, monitor.NewStates.Size())

	monitorService.StopMonitoring(state2, testConn)
	assert.Equal(t, efm.EFM_MONITORS.Size(), 1)
	// States are not tied together. First state remains active as one is cancelled.
	assert.False(t, state2.IsActive())
	assert.True(t, state.IsActive())

	time.Sleep(time.Second) // Let the monitoring routine update.
	assert.Equal(t, 1, monitor.ActiveStates.Size())

	monitorService.StopMonitoring(state, testConn)
	assert.Equal(t, efm.EFM_MONITORS.Size(), 1)
	assert.False(t, state.IsActive())

	assert.Equal(t, 1, monitor.ActiveStates.Size())
	time.Sleep(time.Second) // Let the monitoring routine update.
	assert.Equal(t, 0, monitor.ActiveStates.Size())

	efm.EFM_MONITORS.Clear()
	assert.Equal(t, efm.EFM_MONITORS.Size(), 0)
}

func TestHostMonitoringPluginFactory(t *testing.T) {
	factory := efm.HostMonitoringPluginFactory{}
	_, err := factory.GetInstance(nil, nil)
	// Plugin factory should not return an instance when pluginService is nil.
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "pluginService"))

	pluginService, _, _, _ := beforePluginServiceTests()
	_, err = factory.GetInstance(pluginService, nil)
	// Plugin factory should not return an instance when props is nil.
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "properties"))

	properties := MakeMapFromKeysAndVals(
		property_util.HOST.Name, "host",
	)
	_, err = factory.GetInstance(pluginService, properties)
	// Plugin factory should return an instance given valid parameters.
	assert.Nil(t, err)
}

func mockHostMonitoringPlugin(props *utils.RWMap[string, string]) (*efm.HostMonitorConnectionPlugin, error) {
	factory := efm.HostMonitoringPluginFactory{}
	pluginService, _, _, _ := beforePluginServiceTests()
	if props == nil {
		props = MakeMapFromKeysAndVals(
			property_util.USER.Name, "user",
			property_util.PASSWORD.Name, "password",
			property_util.PORT.Name, "5432",
			property_util.HOST.Name, "host",
			property_util.DATABASE.Name, "dbName",
			property_util.PLUGINS.Name, "test",
		)
	}

	plugin, err := factory.GetInstance(pluginService, props)
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
	assert.Zero(t, efm.EFM_MONITORS.Size())
	assert.Zero(t, queryCounter)

	_, _, _, err = plugin.Execute(nil, utils.CONN_CLOSE, incrementQueryCounter)
	assert.Nil(t, err)

	// When method to be executed is not network bound, no monitoring occurs.
	assert.Zero(t, efm.EFM_MONITORS.Size())
	assert.Equal(t, 1, queryCounter)
}

func TestHostMonitoringPluginExecuteMonitoringEnabled(t *testing.T) {
	plugin, err := mockHostMonitoringPlugin(emptyProps)
	assert.Nil(t, err)
	assert.Zero(t, efm.EFM_MONITORS.Size())
	assert.Zero(t, queryCounter)

	_, _, _, err = plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, incrementQueryCounter)
	assert.Nil(t, err)
	assert.Equal(t, 1, efm.EFM_MONITORS.Size())
	assert.Equal(t, 1, queryCounter)
}

func TestHostMonitoringPluginExecuteThrowsError(t *testing.T) {
	factory := efm.HostMonitoringPluginFactory{}
	pluginService, _, _, _ := beforePluginServiceTests()
	plugin, _ := factory.GetInstance(pluginService, MakeMapFromKeysAndVals("a", "1"))
	// Reset caches and query counter.
	awssql.ClearCaches()
	queryCounter = 0

	assert.Zero(t, efm.EFM_MONITORS.Size())
	assert.Zero(t, queryCounter)

	_, _, _, err := plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, incrementQueryCounter)
	// Empty plugin service unable to supply a host info to monitor.
	assert.NotNil(t, err)
	assert.Zero(t, efm.EFM_MONITORS.Size())
	assert.Zero(t, queryCounter)
}

func TestMonitorCanDispose(t *testing.T) {
	pluginService := &plugin_helpers.PluginServiceImpl{}
	monitor := efm.NewMonitorImpl(pluginService, mockHostInfo, emptyProps, 0, 10, 0, telemetry.NilTelemetryCounter{})
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
	pluginService := &plugin_helpers.PluginServiceImpl{}
	monitor := efm.NewMonitorImpl(pluginService, mockHostInfo, emptyProps, 0, 10, 0, telemetry.NilTelemetryCounter{})
	mockConn := &MockDriverConnection{}
	monitor.MonitoringConn = mockConn

	assert.False(t, monitor.Stopped.Load())
	assert.False(t, mockConn.IsClosed)
	monitor.Close()
	assert.True(t, monitor.Stopped.Load())
	assert.True(t, mockConn.IsClosed)
}

func TestMonitorCheckConnectionStatusOpenConnection(t *testing.T) {
	_, pluginService := initializeTest(map[string]string{property_util.DRIVER_PROTOCOL.Name: "mysql"}, true, false, false, false, false, false)
	monitor := efm.NewMonitorImpl(pluginService, mockHostInfo, emptyProps, 0, 10, 0, telemetry.NilTelemetryCounter{})
	monitor.MonitoringConn = &MockConn{isInvalid: true}

	assert.True(t, monitor.CheckConnectionStatus())
	monitor.Close()
}

func TestMonitorCheckConnectionStatusOpenConnectionFails(t *testing.T) {
	_, pluginService := initializeTest(map[string]string{property_util.DRIVER_PROTOCOL.Name: "mysql"}, true, false, false, true, false, false)
	monitor := efm.NewMonitorImpl(pluginService, mockHostInfo, emptyProps, 0, 10, 0, telemetry.NilTelemetryCounter{})

	monitor.MonitoringConn = MockDriverConn{}
	assert.False(t, monitor.CheckConnectionStatus())
}

func TestMonitorCheckConnectionStatusIsReachable(t *testing.T) {
	_, pluginService := initializeTest(map[string]string{property_util.DRIVER_PROTOCOL.Name: "mysql"}, true, false, false, false, false, false)
	monitor := efm.NewMonitorImpl(pluginService, mockHostInfo, emptyProps, 0, 10, 0, telemetry.NilTelemetryCounter{})

	monitor.MonitoringConn = &MockConn{throwError: false}
	assert.True(t, monitor.CheckConnectionStatus())

	monitor.MonitoringConn = &MockConn{throwError: true}
	assert.False(t, monitor.CheckConnectionStatus())
	monitor.Close()
}

func TestMonitorCheckConnectionStatusNewConn(t *testing.T) {
	pluginService, _, _, _ := beforePluginServiceTests()
	monitor := efm.NewMonitorImpl(pluginService, mockHostInfo, emptyProps, 0, 10, 0, telemetry.NilTelemetryCounter{})

	assert.True(t, monitor.CheckConnectionStatus())
	monitor.Close()
}

func TestMonitorNewConnWithMonitoringProperties(t *testing.T) {
	pluginService, mockPluginManager, _, _ := beforePluginServiceTests()
	props := MakeMapFromKeysAndVals(
		"host", "host",
		"port", "1234",
		"user", "user",
		"password", "password",
		"monitoring-user", "monitor-user",
		"monitoring-password", "monitor-password",
	)
	monitor := efm.NewMonitorImpl(pluginService, mockHostInfo, props, 0, 10, 0, telemetry.NilTelemetryCounter{})
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
	pluginService := &plugin_helpers.PluginServiceImpl{}
	monitor := efm.NewMonitorImpl(pluginService, mockHostInfo, emptyProps, 0, 10, 0, telemetry.NilTelemetryCounter{})
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
	pluginService := &plugin_helpers.PluginServiceImpl{}
	monitor := efm.NewMonitorImpl(pluginService, mockHostInfo, emptyProps, 0, 10, 0, telemetry.NilTelemetryCounter{})
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
