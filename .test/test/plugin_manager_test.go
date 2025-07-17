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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	awsDriver "github.com/aws/aws-advanced-go-wrapper/awssql/driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecuteFunctionCallA(t *testing.T) {
	mockTargetDriver := &MockTargetDriver{}
	props := make(map[string]string)
	connectionProviderManager := driver_infrastructure.ConnectionProviderManager{}
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	target := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, props, connectionProviderManager, telemetryFactory)
	pluginService := driver_infrastructure.PluginService(&plugin_helpers.PluginServiceImpl{})
	var calls []string

	plugins := []driver_infrastructure.ConnectionPlugin{
		CreateTestPlugin(&calls, 1, nil, nil, false),
		CreateTestPlugin(&calls, 2, nil, nil, false),
		CreateTestPlugin(&calls, 3, nil, nil, false),
	}
	err := target.Init(pluginService, plugins)
	if err != nil {
		return
	}

	execFunc := func() (any, any, bool, error) {
		calls = append(calls, "targetCall")
		return "resultTestValue", nil, true, nil
	}

	result, _, _, _ := target.Execute(nil, "callA", execFunc, 10, "arg2, 3.33")
	if result != "resultTestValue" {
		t.Fatalf(`Result should be "resultTestValue", got "%s"`, result)
	}
	if len(calls) != 7 {
		t.Fatalf(`Should have 7 calls, got "%v"`, len(calls))
	}
	if calls[0] != "test.TestPlugin1:before" ||
		calls[1] != "test.TestPlugin2:before" ||
		calls[2] != "test.TestPlugin3:before" ||
		calls[3] != "targetCall" ||
		calls[4] != "test.TestPlugin3:after" ||
		calls[5] != "test.TestPlugin2:after" ||
		calls[6] != "test.TestPlugin1:after" {
		t.Fatalf(`Incorrect calls, got "%v"`, calls)
	}
}

func TestExecuteFunctionCallB(t *testing.T) {
	mockTargetDriver := &MockTargetDriver{}
	props := make(map[string]string)
	connectionProviderManager := driver_infrastructure.ConnectionProviderManager{}
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	target := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, props, connectionProviderManager, telemetryFactory)
	pluginServiceImpl := driver_infrastructure.PluginService(&plugin_helpers.PluginServiceImpl{})
	var calls []string

	plugins := []driver_infrastructure.ConnectionPlugin{
		CreateTestPlugin(&calls, 1, nil, nil, false),
		CreateTestPlugin(&calls, 2, nil, nil, false),
		CreateTestPlugin(&calls, 3, nil, nil, false),
	}
	err := target.Init(pluginServiceImpl, plugins)
	if err != nil {
		return
	}

	execFunc := func() (any, any, bool, error) {
		calls = append(calls, "targetCall")
		return "resultTestValue", nil, true, nil
	}

	result, _, _, _ := target.Execute(nil, "callB", execFunc, 10, "arg2, 3.33")
	if result != "resultTestValue" {
		t.Fatalf(`Result should be "resultTestValue", got "%s"`, result)
	}
	if len(calls) != 5 {
		t.Fatalf(`Should have 5 calls, got "%v"`, len(calls))
	}
	if calls[0] != "test.TestPlugin1:before" ||
		calls[1] != "test.TestPlugin2:before" ||
		calls[2] != "targetCall" ||
		calls[3] != "test.TestPlugin2:after" ||
		calls[4] != "test.TestPlugin1:after" {
		t.Fatalf(`Incorrect calls, got "%v"`, calls)
	}
}

func TestExecuteFunctionCallC(t *testing.T) {
	mockTargetDriver := &MockTargetDriver{}
	props := make(map[string]string)
	connectionProviderManager := driver_infrastructure.ConnectionProviderManager{}
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	target := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, props, connectionProviderManager, telemetryFactory)
	pluginServiceImpl := driver_infrastructure.PluginService(&plugin_helpers.PluginServiceImpl{})
	var calls []string

	plugins := []driver_infrastructure.ConnectionPlugin{
		CreateTestPlugin(&calls, 1, nil, nil, false),
		CreateTestPlugin(&calls, 2, nil, nil, false),
		CreateTestPlugin(&calls, 3, nil, nil, false),
	}
	err := target.Init(pluginServiceImpl, plugins)
	if err != nil {
		return
	}

	execFunc := func() (any, any, bool, error) {
		calls = append(calls, "targetCall")
		return "resultTestValue", nil, true, nil
	}

	result, _, _, _ := target.Execute(nil, "callC", execFunc, 10, "arg2, 3.33")
	if result != "resultTestValue" {
		t.Fatalf(`Result should be "resultTestValue", got "%s"`, result)
	}
	if len(calls) != 3 {
		t.Fatalf(`Should have 3 calls, got "%v"`, len(calls))
	}
	if calls[0] != "test.TestPlugin1:before" ||
		calls[1] != "targetCall" ||
		calls[2] != "test.TestPlugin1:after" {
		t.Fatalf(`Incorrect calls, got "%v"`, calls)
	}
}

func TestConnect(t *testing.T) {
	mockTargetDriver := &MockTargetDriver{}
	props := make(map[string]string)
	connectionProviderManager := driver_infrastructure.ConnectionProviderManager{}
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	target := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, props, connectionProviderManager, telemetryFactory)
	pluginServiceImpl := driver_infrastructure.PluginService(&plugin_helpers.PluginServiceImpl{})
	var calls []string
	expectedConn := &MockDriverConnection{id: 123}

	plugins := []driver_infrastructure.ConnectionPlugin{
		CreateTestPlugin(&calls, 1, nil, nil, false),
		CreateTestPlugin(&calls, 2, nil, nil, false),
		CreateTestPlugin(&calls, 3, expectedConn, nil, false),
	}
	err := target.Init(pluginServiceImpl, plugins)
	if err != nil {
		return
	}

	_, _ = target.Connect(nil, props, true, nil)
	if len(calls) != 4 {
		t.Fatalf(`Should have 4 calls, got "%v"`, len(calls))
	}
	if calls[0] != "test.TestPlugin1:before connect" ||
		calls[1] != "test.TestPlugin3:before connect" ||
		calls[2] != "test.TestPlugin3:connection" ||
		calls[3] != "test.TestPlugin1:after connect" {
		t.Fatalf(`Incorrect calls, got "%v"`, calls)
	}
}

func TestForceConnect(t *testing.T) {
	mockTargetDriver := &MockTargetDriver{}
	props := make(map[string]string)
	connectionProviderManager := driver_infrastructure.ConnectionProviderManager{}
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	target := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, props, connectionProviderManager, telemetryFactory)
	pluginServiceImpl := driver_infrastructure.PluginService(&plugin_helpers.PluginServiceImpl{})
	var calls []string
	expectedConn := &MockDriverConnection{id: 123}

	plugins := []driver_infrastructure.ConnectionPlugin{
		CreateTestPlugin(&calls, 1, nil, nil, false),
		CreateTestPlugin(&calls, 2, nil, nil, false),
		CreateTestPlugin(&calls, 3, expectedConn, nil, false),
	}
	err := target.Init(pluginServiceImpl, plugins)
	if err != nil {
		return
	}

	_, _ = target.ForceConnect(nil, props, true)
	if len(calls) != 4 {
		t.Fatalf(`Should have 4 calls, got "%v"`, len(calls))
	}
	if calls[0] != "test.TestPlugin1:before forceConnect" ||
		calls[1] != "test.TestPlugin3:before forceConnect" ||
		calls[2] != "test.TestPlugin3:forced connection" ||
		calls[3] != "test.TestPlugin1:after forceConnect" {
		t.Fatalf(`Incorrect calls, got "%v"`, calls)
	}
}

func TestConnectWithErrorBefore(t *testing.T) {
	expectedError := errors.New("test error")
	mockTargetDriver := &MockTargetDriver{}
	props := make(map[string]string)
	connectionProviderManager := driver_infrastructure.ConnectionProviderManager{}
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	target := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, props, connectionProviderManager, telemetryFactory)
	pluginServiceImpl := driver_infrastructure.PluginService(&plugin_helpers.PluginServiceImpl{})
	var calls []string
	expectedConn := &MockDriverConnection{id: 123}

	plugins := []driver_infrastructure.ConnectionPlugin{
		CreateTestPlugin(&calls, 1, nil, nil, false),
		CreateTestPlugin(&calls, 2, nil, nil, false),
		CreateTestPlugin(&calls, 1, nil, expectedError, true),
		CreateTestPlugin(&calls, 3, expectedConn, nil, false),
	}

	err := target.Init(pluginServiceImpl, plugins)
	if err != nil {
		return
	}

	_, _ = target.Connect(nil, props, true, nil)
	if len(calls) != 3 {
		t.Fatalf(`Should have 3 calls, got "%v"`, len(calls))
	}
	if calls[0] != "test.TestPlugin1:before connect" ||
		calls[1] != "test.TestPlugin1:before connect" ||
		calls[2] != "test.TestPlugin1:after connect" {
		t.Fatalf(`Incorrect calls, got "%v"`, calls)
	}
}

func TestConnectWithErrorAfter(t *testing.T) {
	expectedError := errors.New("test error")
	mockTargetDriver := &MockTargetDriver{}
	props := make(map[string]string)
	connectionProviderManager := driver_infrastructure.ConnectionProviderManager{}
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	target := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, props, connectionProviderManager, telemetryFactory)
	pluginServiceImpl := driver_infrastructure.PluginService(&plugin_helpers.PluginServiceImpl{})
	var calls []string
	expectedConn := &MockDriverConnection{id: 123}

	plugins := []driver_infrastructure.ConnectionPlugin{
		CreateTestPlugin(&calls, 1, nil, nil, false),
		CreateTestPlugin(&calls, 2, nil, nil, false),
		CreateTestPlugin(&calls, 1, nil, expectedError, false),
		CreateTestPlugin(&calls, 3, expectedConn, nil, true),
	}
	err := target.Init(pluginServiceImpl, plugins)
	if err != nil {
		return
	}

	_, _ = target.Connect(nil, props, true, nil)
	if len(calls) != 5 {
		t.Fatalf(`Should have 5 calls, got "%v"`, len(calls))
	}
	if calls[0] != "test.TestPlugin1:before connect" ||
		calls[1] != "test.TestPlugin1:before connect" ||
		calls[2] != "test.TestPlugin3:before connect" ||
		calls[3] != "test.TestPlugin3:connection" ||
		calls[4] != "test.TestPlugin1:after connect" {
		t.Fatalf(`Incorrect calls, got "%v"`, calls)
	}
}

func TestTwoConnectionsDoNotBlockOneAnother(t *testing.T) {
	routinesWg := new(sync.WaitGroup)
	routinesWg.Add(2)

	waitForDbResourceLocked := new(sync.WaitGroup)
	waitForDbResourceLocked.Add(1)

	waitForReleaseDbResourceToProceed := new(sync.WaitGroup)
	waitForReleaseDbResourceToProceed.Add(1)

	mockTargetDriver := &MockTargetDriver{}
	props := make(map[string]string)
	connectionProviderManager := driver_infrastructure.ConnectionProviderManager{}
	pluginServiceImpl := driver_infrastructure.PluginService(&plugin_helpers.PluginServiceImpl{})

	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	pluginManager1 := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, props, connectionProviderManager, telemetryFactory)
	pluginManager2 := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, props, connectionProviderManager, telemetryFactory)

	var calls []string
	plugins := []driver_infrastructure.ConnectionPlugin{
		CreateTestPlugin(&calls, 1, nil, nil, false),
	}

	err := pluginManager1.Init(pluginServiceImpl, plugins)
	if err != nil {
		t.Fatal(err)
	}
	err = pluginManager2.Init(pluginServiceImpl, plugins)
	if err != nil {
		t.Fatal(err)
	}

	dbResourceMutex := &sync.Mutex{}
	dbResourceReleased := atomic.Bool{}
	acquireDbResourceMutexSuccessful := atomic.Bool{}

	group1 := func(_ *sync.WaitGroup) {
		_, _, _, _ = awsDriver.ExecuteWithPlugins(nil, pluginManager1, "lock-routine-1",
			func() (any, any, bool, error) {
				dbResourceMutex.Lock()
				waitForDbResourceLocked.Done()
				return 1, nil, true, nil
			},
		)
		waitForReleaseDbResourceToProceed.Wait()
		_, _, _, _ = awsDriver.ExecuteWithPlugins(nil, pluginManager1, "release-routine-1",
			func() (any, any, bool, error) {
				dbResourceMutex.Unlock()
				dbResourceReleased.Store(true)
				return 1, nil, true, nil
			},
		)
		routinesWg.Done()
	}

	group2 := func(_ *sync.WaitGroup) {
		waitForDbResourceLocked.Wait()
		_, _, _, _ = awsDriver.ExecuteWithPlugins(nil, pluginManager2, "lock-routine-2",
			func() (any, any, bool, error) {
				waitForReleaseDbResourceToProceed.Done()
				time.Sleep(3 * time.Second)
				tryLock := dbResourceMutex.TryLock()
				acquireDbResourceMutexSuccessful.Store(tryLock)
				return 1, nil, true, nil
			},
		)
		dbResourceMutex.Unlock()
		routinesWg.Done()
	}

	go group1(routinesWg)
	go group2(routinesWg)

	routinesWg.Wait()

	if !dbResourceReleased.Load() {
		t.Fatalf(`Expected dbResourceReleased to be true, got false`)
	}
	if !acquireDbResourceMutexSuccessful.Load() {
		t.Fatalf(`Expected acquireDbResourceMutexSuccessful to be true, got false`)
	}
}

func TestGetHostInfoByStrategyPluginNotSubscribed(t *testing.T) {
	host0, _ := host_info_util.NewHostInfoBuilder().SetHost("host0").SetWeight(10).Build()
	host1, _ := host_info_util.NewHostInfoBuilder().SetHost("host1").SetWeight(9).Build()
	host2, _ := host_info_util.NewHostInfoBuilder().SetHost("host2").SetWeight(8).Build()
	hostList := []*host_info_util.HostInfo{host0, host1, host2}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockPlugin := mock_driver_infrastructure.NewMockConnectionPlugin(ctrl)
	mockPlugin.EXPECT().GetSubscribedMethods().Return([]string{}).AnyTimes()
	mockPlugin.EXPECT().GetHostInfoByStrategy(gomock.Any(), gomock.Any(), gomock.Any()).Return(host0, nil).AnyTimes()

	strategy := "random"
	props := make(map[string]string)
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	pluginManager := plugin_helpers.NewPluginManagerImpl(&MockTargetDriver{}, props, driver_infrastructure.ConnectionProviderManager{}, telemetryFactory)
	err := pluginManager.Init(mockPluginService, []driver_infrastructure.ConnectionPlugin{mockPlugin})
	if err != nil {
		return
	}

	selectedHost, err := pluginManager.GetHostInfoByStrategy(host_info_util.WRITER, strategy, hostList)

	assert.Nil(t, selectedHost)
	require.NotNil(t, err)
	assert.Equal(t, error_util.GetMessage("PluginManagerImpl.unsupportedHostSelectionStrategy", strategy), err.Error())
}

func TestGetHostInfoByStrategyPluginDiffSubscribed(t *testing.T) {
	host0, _ := host_info_util.NewHostInfoBuilder().SetHost("host0").SetWeight(10).Build()
	host1, _ := host_info_util.NewHostInfoBuilder().SetHost("host1").SetWeight(9).Build()
	host2, _ := host_info_util.NewHostInfoBuilder().SetHost("host2").SetWeight(8).Build()
	hostList := []*host_info_util.HostInfo{host0, host1, host2}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPlugin := mock_driver_infrastructure.NewMockConnectionPlugin(ctrl)
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)

	mockPlugin.EXPECT().GetSubscribedMethods().Return([]string{plugin_helpers.CONNECT_METHOD}).AnyTimes()
	mockPlugin.EXPECT().GetHostInfoByStrategy(gomock.Any(), gomock.Any(), gomock.Any()).Return(host0, nil).AnyTimes()

	strategy := "random"
	props := make(map[string]string)
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	pluginManager := plugin_helpers.NewPluginManagerImpl(&MockTargetDriver{}, props, driver_infrastructure.ConnectionProviderManager{}, telemetryFactory)
	err := pluginManager.Init(mockPluginService, []driver_infrastructure.ConnectionPlugin{mockPlugin})
	if err != nil {
		return
	}
	selectedHost, err := pluginManager.GetHostInfoByStrategy(host_info_util.WRITER, strategy, hostList)

	assert.Nil(t, selectedHost)
	require.NotNil(t, err)
	assert.Equal(t, error_util.GetMessage("PluginManagerImpl.unsupportedHostSelectionStrategy", strategy), err.Error())
}

func TestGetHostInfoByStrategyPluginUnsupported(t *testing.T) {
	host0, _ := host_info_util.NewHostInfoBuilder().SetHost("host0").SetWeight(10).Build()
	host1, _ := host_info_util.NewHostInfoBuilder().SetHost("host1").SetWeight(9).Build()
	host2, _ := host_info_util.NewHostInfoBuilder().SetHost("host2").SetWeight(8).Build()
	hostList := []*host_info_util.HostInfo{host0, host1, host2}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPlugin := mock_driver_infrastructure.NewMockConnectionPlugin(ctrl)
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)

	mockPlugin.EXPECT().GetSubscribedMethods().Return([]string{plugin_helpers.ALL_METHODS}).AnyTimes()
	testError := errors.New("test")
	mockPlugin.EXPECT().GetHostInfoByStrategy(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, testError).AnyTimes()

	strategy := "random"
	props := make(map[string]string)
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	pluginManager := plugin_helpers.NewPluginManagerImpl(&MockTargetDriver{}, props, driver_infrastructure.ConnectionProviderManager{}, telemetryFactory)
	err := pluginManager.Init(mockPluginService, []driver_infrastructure.ConnectionPlugin{mockPlugin})
	if err != nil {
		return
	}
	selectedHost, err := pluginManager.GetHostInfoByStrategy(host_info_util.WRITER, strategy, hostList)

	assert.Nil(t, selectedHost)
	require.NotNil(t, err)
	assert.Equal(t, testError, err)
}

func TestGetHostInfoByStrategyPluginSupported(t *testing.T) {
	host0, _ := host_info_util.NewHostInfoBuilder().SetHost("host0").SetWeight(10).Build()
	host1, _ := host_info_util.NewHostInfoBuilder().SetHost("host1").SetWeight(9).Build()
	host2, _ := host_info_util.NewHostInfoBuilder().SetHost("host2").SetWeight(8).Build()
	hostList := []*host_info_util.HostInfo{host0, host1, host2}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPlugin := mock_driver_infrastructure.NewMockConnectionPlugin(ctrl)
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)

	mockPlugin.EXPECT().GetSubscribedMethods().Return([]string{plugin_helpers.ALL_METHODS}).AnyTimes()
	mockPlugin.EXPECT().GetHostInfoByStrategy(gomock.Any(), gomock.Any(), gomock.Any()).Return(host0, nil).AnyTimes()

	strategy := "random"
	props := make(map[string]string)
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	pluginManager := plugin_helpers.NewPluginManagerImpl(&MockTargetDriver{}, props, driver_infrastructure.ConnectionProviderManager{}, telemetryFactory)
	err := pluginManager.Init(mockPluginService, []driver_infrastructure.ConnectionPlugin{mockPlugin})
	if err != nil {
		return
	}
	selectedHost, err := pluginManager.GetHostInfoByStrategy(host_info_util.WRITER, strategy, hostList)

	assert.Nil(t, err)
	require.NotNil(t, selectedHost)
	assert.Equal(t, host0, selectedHost)
}

func TestGetHostInfoByStrategyMultiplePlugins(t *testing.T) {
	host0, _ := host_info_util.NewHostInfoBuilder().SetHost("host0").SetWeight(10).Build()
	host1, _ := host_info_util.NewHostInfoBuilder().SetHost("host1").SetWeight(9).Build()
	host2, _ := host_info_util.NewHostInfoBuilder().SetHost("host2").SetWeight(8).Build()
	hostList := []*host_info_util.HostInfo{host0, host1, host2}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	unsubscribedPlugin0 := mock_driver_infrastructure.NewMockConnectionPlugin(ctrl)
	unsubscribedPlugin0.EXPECT().GetSubscribedMethods().Return([]string{}).AnyTimes()
	unsubscribedPlugin0.EXPECT().GetHostInfoByStrategy(gomock.Any(), gomock.Any(), gomock.Any()).Return(host0, nil).AnyTimes()

	unsubscribedPlugin1 := mock_driver_infrastructure.NewMockConnectionPlugin(ctrl)
	unsubscribedPlugin1.EXPECT().GetSubscribedMethods().Return([]string{plugin_helpers.CONNECT_METHOD}).AnyTimes()
	unsubscribedPlugin1.EXPECT().GetHostInfoByStrategy(gomock.Any(), gomock.Any(), gomock.Any()).Return(host0, nil).AnyTimes()

	unsupportedSubscribedPlugin0 := mock_driver_infrastructure.NewMockConnectionPlugin(ctrl)
	unsupportedSubscribedPlugin0.EXPECT().GetSubscribedMethods().Return([]string{plugin_helpers.ALL_METHODS}).AnyTimes()
	unsupportedSubscribedPlugin0.EXPECT().GetHostInfoByStrategy(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("test")).AnyTimes()

	unsupportedSubscribedPlugin1 := mock_driver_infrastructure.NewMockConnectionPlugin(ctrl)
	unsupportedSubscribedPlugin1.EXPECT().GetSubscribedMethods().Return([]string{plugin_helpers.GET_HOST_INFO_BY_STRATEGY_METHOD}).AnyTimes()
	unsupportedSubscribedPlugin1.EXPECT().GetHostInfoByStrategy(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("test")).AnyTimes()

	supportedSubscribedPlugin := mock_driver_infrastructure.NewMockConnectionPlugin(ctrl)
	supportedSubscribedPlugin.EXPECT().GetSubscribedMethods().Return([]string{plugin_helpers.GET_HOST_INFO_BY_STRATEGY_METHOD}).AnyTimes()
	supportedSubscribedPlugin.EXPECT().GetHostInfoByStrategy(gomock.Any(), gomock.Any(), gomock.Any()).Return(host1, nil).AnyTimes()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	plugins := []driver_infrastructure.ConnectionPlugin{
		unsubscribedPlugin0,
		unsubscribedPlugin1,
		unsupportedSubscribedPlugin0,
		unsupportedSubscribedPlugin1,
		supportedSubscribedPlugin,
	}

	strategy := "random"
	props := make(map[string]string)
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	pluginManager := plugin_helpers.NewPluginManagerImpl(&MockTargetDriver{}, props, driver_infrastructure.ConnectionProviderManager{}, telemetryFactory)
	err := pluginManager.Init(mockPluginService, plugins)
	if err != nil {
		return
	}
	selectedHost, err := pluginManager.GetHostInfoByStrategy(host_info_util.WRITER, strategy, hostList)

	assert.Nil(t, err)
	require.NotNil(t, selectedHost)
	assert.Equal(t, host1, selectedHost)
}

func TestConnectPluginToSkip(t *testing.T) {
	hostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("host0").SetWeight(10).Build()
	mockConn := MockDriverConn{}
	testErr := errors.New("test")

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pluginThrowsError := mock_driver_infrastructure.NewMockConnectionPlugin(ctrl)
	pluginThrowsError.EXPECT().GetSubscribedMethods().Return([]string{plugin_helpers.CONNECT_METHOD}).AnyTimes()
	pluginThrowsError.EXPECT().Connect(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, testErr).AnyTimes()

	pluginConnects := mock_driver_infrastructure.NewMockConnectionPlugin(ctrl)
	pluginConnects.EXPECT().GetSubscribedMethods().Return([]string{plugin_helpers.CONNECT_METHOD}).AnyTimes()
	pluginConnects.EXPECT().Connect(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(mockConn, nil).AnyTimes()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)

	props := make(map[string]string)
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	pluginManager := plugin_helpers.NewPluginManagerImpl(&MockTargetDriver{}, props, driver_infrastructure.ConnectionProviderManager{}, telemetryFactory)

	err := pluginManager.Init(mockPluginService, []driver_infrastructure.ConnectionPlugin{pluginThrowsError, pluginConnects})
	require.Nil(t, err)

	// When skipping the plugin that connects it should receive the error from the plugin that throws the error.
	conn, err := pluginManager.Connect(hostInfo, props, false, pluginConnects)

	assert.Nil(t, conn)
	require.NotNil(t, err)
	assert.Equal(t, testErr, err)

	// When skipping the plugin that throws the error it should connect.
	conn, err = pluginManager.Connect(hostInfo, props, false, pluginThrowsError)

	assert.NotNil(t, conn)
	require.Nil(t, err)

	// Plugin chain is not cached, should go through the chain and throw error.
	conn, err = pluginManager.Connect(hostInfo, props, false, nil)

	assert.Nil(t, conn)
	require.NotNil(t, err)
	assert.Equal(t, testErr, err)

	// Now that plugin chain is cached even as plugins change to only ones that will connect, the error will still be thrown.
	err = pluginManager.Init(mockPluginService, []driver_infrastructure.ConnectionPlugin{pluginConnects})
	require.Nil(t, err)

	conn, err = pluginManager.Connect(hostInfo, props, false, nil)

	assert.Nil(t, conn)
	require.NotNil(t, err)
	assert.Equal(t, testErr, err)

	// Will make a new chain given pluginToSkip, will fail because that it is skipping the only plugin.
	conn, err = pluginManager.Connect(hostInfo, props, false, pluginConnects)

	assert.Nil(t, conn)
	require.NotNil(t, err)
	assert.Equal(t, error_util.NewGenericAwsWrapperError(error_util.GetMessage("PluginManager.pipelineNone")), err)
}
