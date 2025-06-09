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
	"awssql/plugin_helpers"
	"awssql/plugins/efm"
	"awssql/property_util"
	"awssql/utils"
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"
	"time"
	"weak"

	"github.com/stretchr/testify/assert"
)

func TestMonitorConnectionState(t *testing.T) {
	var conn driver.Conn = &MockDriverConn{}
	state := efm.NewMonitorConnectionState(&conn)

	assert.NotNil(t, state.GetConn())
	assert.True(t, state.IsActive())
	// By default nodeUnhealthy is false. When node is healthy should not abort.
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
	assert.Nil(t, efm.EFM_MONITORS)

	_, pluginService := initializeTest(map[string]string{property_util.DRIVER_PROTOCOL.Name: "mysql"}, true, false, false, false, false, false)
	monitorService := efm.NewMonitorServiceImpl(pluginService)
	var testConn driver.Conn = &MockConn{throwError: false}

	assert.NotNil(t, efm.EFM_MONITORS)
	assert.Zero(t, efm.EFM_MONITORS.Size())

	_, err := monitorService.StartMonitoring(nil, nil, nil, 0, 0, 0)
	// Monitoring with an invalid conn should fail.
	assert.True(t, strings.Contains(err.Error(), "conn"))

	_, err = monitorService.StartMonitoring(&testConn, nil, nil, 0, 0, 0)
	// Monitoring with an invalid monitoring HostInfo should fail.
	assert.True(t, strings.Contains(err.Error(), "hostInfo"))

	// Monitoring with correct parameters should create a new monitor.
	state, err := monitorService.StartMonitoring(&testConn, mockHostInfo, nil, 0, 900, 3)
	monitorKey := fmt.Sprintf("%d:%d:%d:%s", 0, 900, 3, mockHostInfo.GetUrl())

	assert.Nil(t, err)
	assert.True(t, state.IsActive())
	assert.Equal(t, efm.EFM_MONITORS.Size(), 1)
	val, ok := efm.EFM_MONITORS.Get(monitorKey, time.Minute)
	assert.True(t, ok)
	assert.NotNil(t, val)
	monitor, ok := val.(*efm.MonitorImpl)
	assert.True(t, ok)

	state2, err := monitorService.StartMonitoring(&testConn, mockHostInfo, nil, 0, 900, 3)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(monitor.NewStates))
	// Monitoring on the same host should not increase the cache size.
	assert.Equal(t, efm.EFM_MONITORS.Size(), 1)
	assert.True(t, state2.IsActive())

	monitor.MonitoringConn = testConn
	monitoringConn := &MockConn{}
	monitor.MonitoringConn = monitoringConn

	// Let the newStates monitoring routine update.
	for len(monitor.ActiveStates) != 2 {
		time.Sleep(time.Second)
	}
	assert.Equal(t, 2, len(monitor.ActiveStates))
	assert.Equal(t, 0, len(monitor.NewStates))

	monitorService.StopMonitoring(state2, testConn)
	assert.Equal(t, efm.EFM_MONITORS.Size(), 1)
	// States are not tied together. First state remains active as one is cancelled.
	assert.False(t, state2.IsActive())
	assert.True(t, state.IsActive())

	assert.Equal(t, 2, len(monitor.ActiveStates))
	time.Sleep(time.Second) // Let the monitoring routine update.
	assert.Equal(t, 1, len(monitor.ActiveStates))

	monitorService.StopMonitoring(state, testConn)
	assert.Equal(t, efm.EFM_MONITORS.Size(), 1)
	assert.False(t, state.IsActive())

	assert.Equal(t, 1, len(monitor.ActiveStates))
	time.Sleep(time.Second) // Let the monitoring routine update.
	assert.Equal(t, 0, len(monitor.ActiveStates))

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

	properties := map[string]string{
		property_util.HOST.Name: "host",
	}
	_, err = factory.GetInstance(pluginService, properties)
	// Plugin factory should return an instance given valid parameters.
	assert.Nil(t, err)
}

func mockHostMonitoringPlugin(props map[string]string) (*efm.HostMonitorConnectionPlugin, error) {
	factory := efm.HostMonitoringPluginFactory{}
	pluginService, _, _, _ := beforePluginServiceTests()
	if props == nil {
		props = map[string]string{
			property_util.USER.Name:     "user",
			property_util.PASSWORD.Name: "password",
			property_util.PORT.Name:     "5432",
			property_util.HOST.Name:     "host",
			property_util.DATABASE.Name: "dbName",
			property_util.PLUGINS.Name:  "test",
		}
	}

	plugin, err := factory.GetInstance(pluginService, props)
	if err != nil {
		return nil, err
	}
	err = pluginService.SetCurrentConnection(&MockDriverConnection{}, mockHostInfo, plugin)
	monitoringPlugin, _ := plugin.(*efm.HostMonitorConnectionPlugin)

	// Reset caches and query counter.
	efm.ClearCaches()
	queryCounter = 0
	return monitoringPlugin, err
}

var queryCounter = 0

func incrementQueryCounter() (any, any, bool, error) {
	queryCounter++
	return nil, nil, false, nil
}

func TestHostMonitoringPluginConnect(t *testing.T) {
	plugin, err := mockHostMonitoringPlugin(nil)
	assert.Nil(t, err)
	rdsHostInfo, err := host_info_util.NewHostInfoBuilder().SetHost("instance-a-1.xyz.us-east-2.rds.amazonaws.com").Build()
	assert.Nil(t, err)
	connectFunc := func() (driver.Conn, error) {
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
	plugin, err := mockHostMonitoringPlugin(nil)
	assert.Nil(t, err)
	// Set monitoring HostInfo by executing a network bound method.
	_, _, _, err = plugin.Execute(utils.CONN_QUERY_CONTEXT, incrementQueryCounter)
	assert.Nil(t, err)
	assert.NotNil(t, plugin.GetMonitoringHostInfo())

	// Monitoring HostInfo should be reset when host changes.
	hostChanged := map[driver_infrastructure.HostChangeOptions]bool{driver_infrastructure.HOST_CHANGED: true}
	action := plugin.NotifyConnectionChanged(hostChanged)
	assert.Nil(t, plugin.GetMonitoringHostInfo())
	assert.Equal(t, driver_infrastructure.NO_OPINION, action)

	plugin, err = mockHostMonitoringPlugin(nil)
	assert.Nil(t, err)
	// Set monitoring HostInfo by executing a network bound method.
	_, _, _, err = plugin.Execute(utils.CONN_QUERY_CONTEXT, incrementQueryCounter)
	assert.Nil(t, err)
	assert.NotNil(t, plugin.GetMonitoringHostInfo())

	// Monitoring HostInfo should be reset when hostname changes.
	hostNameChanged := map[driver_infrastructure.HostChangeOptions]bool{driver_infrastructure.HOSTNAME: true}
	action = plugin.NotifyConnectionChanged(hostNameChanged)
	assert.Nil(t, plugin.GetMonitoringHostInfo())
	assert.Equal(t, driver_infrastructure.NO_OPINION, action)
}

func TestHostMonitoringPluginExecuteMonitoringDisabled(t *testing.T) {
	props := map[string]string{property_util.FAILURE_DETECTION_ENABLED.Name: "false"}
	plugin, err := mockHostMonitoringPlugin(props)
	assert.Nil(t, err)
	assert.Zero(t, efm.EFM_MONITORS.Size())
	assert.Zero(t, queryCounter)

	_, _, _, err = plugin.Execute(utils.CONN_QUERY_CONTEXT, incrementQueryCounter)
	assert.Nil(t, err)

	// When monitoring is disabled, function is executed but no monitoring occurs.
	assert.Zero(t, efm.EFM_MONITORS.Size())
	assert.Equal(t, 1, queryCounter)
}

func TestHostMonitoringPluginExecuteMonitoringUnnecessary(t *testing.T) {
	plugin, err := mockHostMonitoringPlugin(nil)
	assert.Nil(t, err)
	assert.Zero(t, efm.EFM_MONITORS.Size())
	assert.Zero(t, queryCounter)

	_, _, _, err = plugin.Execute(utils.CONN_CLOSE, incrementQueryCounter)
	assert.Nil(t, err)

	// When method to be executed is not network bound, no monitoring occurs.
	assert.Zero(t, efm.EFM_MONITORS.Size())
	assert.Equal(t, 1, queryCounter)
}

func TestHostMonitoringPluginExecuteMonitoringEnabled(t *testing.T) {
	plugin, err := mockHostMonitoringPlugin(nil)
	assert.Nil(t, err)
	assert.Zero(t, efm.EFM_MONITORS.Size())
	assert.Zero(t, queryCounter)

	_, _, _, err = plugin.Execute(utils.CONN_QUERY_CONTEXT, incrementQueryCounter)
	assert.Nil(t, err)
	assert.Equal(t, 1, efm.EFM_MONITORS.Size())
	assert.Equal(t, 1, queryCounter)
}

func TestHostMonitoringPluginExecuteThrowsError(t *testing.T) {
	factory := efm.HostMonitoringPluginFactory{}
	pluginService, _, _, _ := beforePluginServiceTests()
	plugin, _ := factory.GetInstance(pluginService, map[string]string{"a": "1"})
	// Reset caches and query counter.
	efm.ClearCaches()
	queryCounter = 0

	assert.Zero(t, efm.EFM_MONITORS.Size())
	assert.Zero(t, queryCounter)

	_, _, _, err := plugin.Execute(utils.CONN_QUERY_CONTEXT, incrementQueryCounter)
	// Empty plugin service unable to supply a host info to monitor.
	assert.NotNil(t, err)
	assert.Zero(t, efm.EFM_MONITORS.Size())
	assert.Zero(t, queryCounter)
}

func TestMonitorCanDispose(t *testing.T) {
	pluginService := &plugin_helpers.PluginServiceImpl{}
	monitor := efm.NewMonitorImpl(pluginService, mockHostInfo, nil, 0, 10, 0)
	monitor.MonitoringConn = &MockDriverConnection{}
	var conn driver.Conn = &MockDriverConn{}
	state := efm.NewMonitorConnectionState(&conn)

	// If there are no states, monitor can be disposed.
	assert.True(t, monitor.CanDispose())

	monitor.NewStates[time.Time{}] = nil
	assert.False(t, monitor.CanDispose())

	monitor.ActiveStates = append(monitor.ActiveStates, weak.Make(state))
	assert.False(t, monitor.CanDispose())

	for t2 := range monitor.NewStates {
		delete(monitor.NewStates, t2)
	}
	assert.False(t, monitor.CanDispose())
	monitor.Close()
}

func TestMonitorClose(t *testing.T) {
	pluginService := &plugin_helpers.PluginServiceImpl{}
	monitor := efm.NewMonitorImpl(pluginService, mockHostInfo, nil, 0, 10, 0)
	mockConn := &MockDriverConnection{}
	monitor.MonitoringConn = mockConn

	assert.False(t, monitor.Stopped)
	assert.False(t, mockConn.IsClosed)
	monitor.Close()
	assert.True(t, monitor.Stopped)
	assert.True(t, mockConn.IsClosed)
}

func TestMonitorCheckConnectionStatusOpenConnection(t *testing.T) {
	_, pluginService := initializeTest(map[string]string{property_util.DRIVER_PROTOCOL.Name: "mysql"}, true, false, false, false, false, false)
	monitor := efm.NewMonitorImpl(pluginService, mockHostInfo, nil, 0, 10, 0)
	monitor.MonitoringConn = &MockConn{isInvalid: true}

	assert.True(t, monitor.CheckConnectionStatus())
	monitor.Close()
}

func TestMonitorCheckConnectionStatusOpenConnectionFails(t *testing.T) {
	_, pluginService := initializeTest(map[string]string{property_util.DRIVER_PROTOCOL.Name: "mysql"}, true, false, false, true, false, false)
	monitor := efm.NewMonitorImpl(pluginService, mockHostInfo, nil, 0, 10, 0)

	monitor.MonitoringConn = MockDriverConn{}
	assert.False(t, monitor.CheckConnectionStatus())
}

func TestMonitorCheckConnectionStatusIsReachable(t *testing.T) {
	_, pluginService := initializeTest(map[string]string{property_util.DRIVER_PROTOCOL.Name: "mysql"}, true, false, false, false, false, false)
	monitor := efm.NewMonitorImpl(pluginService, mockHostInfo, nil, 0, 10, 0)

	monitor.MonitoringConn = &MockConn{throwError: false}
	assert.True(t, monitor.CheckConnectionStatus())

	monitor.MonitoringConn = &MockConn{throwError: true}
	assert.False(t, monitor.CheckConnectionStatus())
	monitor.Close()
}

func TestMonitorCheckConnectionStatusNewConn(t *testing.T) {
	pluginService, _, _, _ := beforePluginServiceTests()
	monitor := efm.NewMonitorImpl(pluginService, mockHostInfo, nil, 0, 10, 0)

	assert.True(t, monitor.CheckConnectionStatus())
	monitor.Close()
}

func TestMonitorNewConnWithMonitoringProperties(t *testing.T) {
	pluginService, mockPluginManager, _, _ := beforePluginServiceTests()
	props := map[string]string{
		"host":                "host",
		"port":                "1234",
		"user":                "user",
		"password":            "password",
		"monitoring-user":     "monitor-user",
		"monitoring-password": "monitor-password",
	}
	monitor := efm.NewMonitorImpl(pluginService, mockHostInfo, props, 0, 10, 0)
	monitor.Close() // Ensures none of the monitoring goroutines are running in the background.

	assert.True(t, monitor.CheckConnectionStatus())
	assert.NotNil(t, mockPluginManager.ForceConnectProps)
	assert.Equal(t, len(mockPluginManager.ForceConnectProps), 4)
	assert.Equal(t, mockPluginManager.ForceConnectProps["host"], "host")
	assert.Equal(t, mockPluginManager.ForceConnectProps["port"], "1234")
	assert.Equal(t, mockPluginManager.ForceConnectProps["user"], "monitor-user")
	assert.Equal(t, mockPluginManager.ForceConnectProps["password"], "monitor-password")
}

func TestMonitorUpdateHostHealthStatusValid(t *testing.T) {
	pluginService := &plugin_helpers.PluginServiceImpl{}
	monitor := efm.NewMonitorImpl(pluginService, mockHostInfo, nil, 0, 10, 0)
	monitor.Close() // Ensures none of the monitoring goroutines are running in the background.

	monitor.FailureCount = 1
	monitor.InvalidHostStartTime = time.Now()
	monitor.HostUnhealthy = true

	monitor.UpdateHostHealthStatus(true, time.Now(), time.Now().Add(5))

	assert.Zero(t, monitor.FailureCount)
	assert.Zero(t, monitor.InvalidHostStartTime)
	assert.False(t, monitor.HostUnhealthy)
	monitor.Close()
}

func TestMonitorUpdateHostHealthStatusInvalid(t *testing.T) {
	pluginService := &plugin_helpers.PluginServiceImpl{}
	monitor := efm.NewMonitorImpl(pluginService, mockHostInfo, nil, 0, 10, 0)
	monitor.Close() // Ensures none of the monitoring goroutines are running in the background.

	assert.Zero(t, monitor.FailureCount)
	assert.Zero(t, monitor.InvalidHostStartTime)
	assert.False(t, monitor.HostUnhealthy)

	startTime := time.Now()
	monitor.UpdateHostHealthStatus(false, startTime, time.Now().Add(5))

	assert.Equal(t, 1, monitor.FailureCount)
	assert.Equal(t, startTime, monitor.InvalidHostStartTime)
	assert.True(t, monitor.HostUnhealthy)
	monitor.Close()
}
