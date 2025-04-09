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
	awsDriver "awssql/driver"
	"awssql/driver_infrastructure"
	"awssql/plugin_helpers"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestExecuteFunctionCallA(t *testing.T) {
	mockTargetDriver := &MockTargetDriver{}
	props := make(map[string]string)
	connectionProviderManager := driver_infrastructure.ConnectionProviderManager{}
	target := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, props, connectionProviderManager)
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

	result, _, _, _ := target.Execute("callA", execFunc, 10, "arg2, 3.33")
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
	target := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, props, connectionProviderManager)
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

	result, _, _, _ := target.Execute("callB", execFunc, 10, "arg2, 3.33")
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
	target := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, props, connectionProviderManager)
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

	result, _, _, _ := target.Execute("callC", execFunc, 10, "arg2, 3.33")
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
	target := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, props, connectionProviderManager)
	pluginServiceImpl := driver_infrastructure.PluginService(&plugin_helpers.PluginServiceImpl{})
	var calls []string
	expectedConn := MockDriverConnection{id: 123}

	plugins := []driver_infrastructure.ConnectionPlugin{
		CreateTestPlugin(&calls, 1, nil, nil, false),
		CreateTestPlugin(&calls, 2, nil, nil, false),
		CreateTestPlugin(&calls, 3, expectedConn, nil, false),
	}
	err := target.Init(pluginServiceImpl, plugins)
	if err != nil {
		return
	}

	_, _ = target.Connect(nil, props, true)
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
	target := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, props, connectionProviderManager)
	pluginServiceImpl := driver_infrastructure.PluginService(&plugin_helpers.PluginServiceImpl{})
	var calls []string
	expectedConn := MockDriverConnection{id: 123}

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
	target := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, props, connectionProviderManager)
	pluginServiceImpl := driver_infrastructure.PluginService(&plugin_helpers.PluginServiceImpl{})
	var calls []string
	expectedConn := MockDriverConnection{id: 123}

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

	_, _ = target.Connect(nil, props, true)
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
	target := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, props, connectionProviderManager)
	pluginServiceImpl := driver_infrastructure.PluginService(&plugin_helpers.PluginServiceImpl{})
	var calls []string
	expectedConn := MockDriverConnection{id: 123}

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

	_, _ = target.Connect(nil, props, true)
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

	pluginManager1 := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, props, connectionProviderManager)
	pluginManager2 := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, props, connectionProviderManager)

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

	group1 := func(wg *sync.WaitGroup) {
		_, _, _, _ = awsDriver.ExecuteWithPlugins(pluginManager1, "lock-routine-1",
			func() (any, any, bool, error) {
				dbResourceMutex.Lock()
				waitForDbResourceLocked.Done()
				return 1, nil, true, nil
			},
		)
		waitForReleaseDbResourceToProceed.Wait()
		_, _, _, _ = awsDriver.ExecuteWithPlugins(pluginManager1, "release-routine-1",
			func() (any, any, bool, error) {
				dbResourceMutex.Unlock()
				dbResourceReleased.Store(true)
				return 1, nil, true, nil
			},
		)
		routinesWg.Done()
	}

	group2 := func(wg *sync.WaitGroup) {
		waitForDbResourceLocked.Wait()
		_, _, _, _ = awsDriver.ExecuteWithPlugins(pluginManager2, "lock-routine-2",
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

/* TODO: complete following tests when corresponding features are complete
- testExecuteCachedJdbcCallA (verify)
- testExecuteAgainstOldConnection
- testDefaultPlugins
- testNoWrapperPlugins
- testOverridingDefaultPluginsWithPluginCodes
- testGetHostSpecByStrategy_givenPluginWithNoSubscriptions_thenThrowsSqlException
- testGetHostSpecByStrategy_givenPluginWithDiffSubscription_thenThrowsSqlException
- testGetHostSpecByStrategy_givenUnsupportedPlugin_thenThrowsSqlException
- testGetHostSpecByStrategy_givenSupportedSubscribedPlugin_thenThrowsSqlException
- testGetHostSpecByStrategy_givenMultiplePlugins
- testGetHostSpecByStrategy_givenInputHostsAndMultiplePlugins
*/
