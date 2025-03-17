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
	"awssql/host_info_util"
	"awssql/plugin_helpers"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type TestPlugin struct {
	calls      *[]string
	id         int
	connection driver.Conn
	error      error
	isBefore   bool
}

func (t TestPlugin) GetSubscribedMethods() []string {
	switch t.id {
	case 1:
		return []string{"*"}
	case 2:
		return []string{"callA", "callB"}
	case 3:
		return []string{"callA", "forceConnect", "connect"}
	default:
		return []string{"*"}
	}
}

func (t TestPlugin) Execute(methodName string, executeFunc driver_infrastructure.ExecuteFunc, methodArgs ...any) (any, any, bool, error) {
	*t.calls = append(*t.calls, fmt.Sprintf("%s%v:before", reflect.TypeOf(t), t.id))
	if t.isBefore && t.error != nil {
		return nil, nil, false, t.error
	}
	result, _, _, err := executeFunc()
	*t.calls = append(*t.calls, fmt.Sprintf("%s%v:after", reflect.TypeOf(t), t.id))
	if !t.isBefore && t.error != nil {
		return nil, nil, false, t.error
	}
	return result, nil, true, err
}

func (t TestPlugin) Connect(
	hostInfo host_info_util.HostInfo,
	properties map[string]string,
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	*t.calls = append(*t.calls, fmt.Sprintf("%s%v:before connect", reflect.TypeOf(t), t.id))
	if t.isBefore && t.error != nil {
		return nil, t.error
	}
	if t.connection != nil {
		*t.calls = append(*t.calls, fmt.Sprintf("%s%v:connection", reflect.TypeOf(t), t.id))
		return t.connection, nil
	}
	result, err := connectFunc()
	if !t.isBefore && t.error != nil {
		return nil, t.error
	}
	*t.calls = append(*t.calls, fmt.Sprintf("%s%v:after connect", reflect.TypeOf(t), t.id))
	conn, ok := result.(driver.Conn)
	if ok {
		return conn, err
	}
	return nil, err
}

func (t TestPlugin) ForceConnect(
	hostInfo host_info_util.HostInfo,
	properties map[string]string,
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	*t.calls = append(*t.calls, fmt.Sprintf("%s%v:before forceConnect", reflect.TypeOf(t), t.id))
	if t.connection != nil {
		*t.calls = append(*t.calls, fmt.Sprintf("%s%v:forced connection", reflect.TypeOf(t), t.id))
		return t.connection, nil
	}
	result, err := connectFunc()
	*t.calls = append(*t.calls, fmt.Sprintf("%s%v:after forceConnect", reflect.TypeOf(t), t.id))
	conn, ok := result.(driver.Conn)
	if ok {
		return conn, err
	}
	return nil, err
}

func (t TestPlugin) AcceptsStrategy(role host_info_util.HostRole, strategy string) bool {
	return false
}

func (t TestPlugin) GetHostInfoByStrategy(
	role host_info_util.HostRole,
	strategy string,
	hosts []host_info_util.HostInfo) (host_info_util.HostInfo, error) {
	*t.calls = append(*t.calls, fmt.Sprintf("%s%v:before GetHostInfoByStrategy", reflect.TypeOf(t), t.id))
	result := host_info_util.HostInfo{}
	*t.calls = append(*t.calls, fmt.Sprintf("%s%v:after GetHostInfoByStrategy", reflect.TypeOf(t), t.id))
	return result, nil
}

func (t TestPlugin) NotifyConnectionChanged(changes map[driver_infrastructure.HostChangeOptions]bool) driver_infrastructure.OldConnectionSuggestedAction {
	return driver_infrastructure.NO_OPINION
}

func (t TestPlugin) NotifyHostListChanged(changes map[string]map[driver_infrastructure.HostChangeOptions]bool) {
	// Do nothing
}

func (t TestPlugin) InitHostProvider(
	initialUrl string,
	props map[string]string,
	hostListProviderService *driver_infrastructure.HostListProviderService,
	initHostProviderFunc func() error) error {
	// Do nothing
	return nil
}

type MockTargetDriver struct{}

func (m MockTargetDriver) Open(name string) (driver.Conn, error) {
	return nil, nil
}

type MockDriverConnection struct {
	id int
}

func (m MockDriverConnection) Prepare(query string) (driver.Stmt, error) {
	// Do nothing.
	return nil, nil
}

func (m MockDriverConnection) Close() error {
	// Do nothing.
	return nil
}

func (m MockDriverConnection) Begin() (driver.Tx, error) {
	// Do nothing.
	return nil, nil
}

func CreateTestPlugin(calls *[]string, id int, connection driver.Conn, err error, isBefore bool) *driver_infrastructure.ConnectionPlugin {
	if calls == nil {
		calls = &[]string{}
	}
	testPlugin := driver_infrastructure.ConnectionPlugin(&TestPlugin{calls: calls, id: id, connection: connection, error: err, isBefore: isBefore})
	return &testPlugin
}

func TestExecuteFunctionCallA(t *testing.T) {
	mockTargetDriver := &MockTargetDriver{}
	props := make(map[string]string)
	driverConnProvider := driver_infrastructure.NewDriverConnectionProvider(mockTargetDriver)
	defaultConnProvider := driver_infrastructure.ConnectionProvider(driverConnProvider)
	target := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, &defaultConnProvider, nil, props)
	pluginServiceImpl := driver_infrastructure.PluginService(&plugin_helpers.PluginServiceImpl{})
	var calls []string

	plugins := []*driver_infrastructure.ConnectionPlugin{
		CreateTestPlugin(&calls, 1, nil, nil, false),
		CreateTestPlugin(&calls, 2, nil, nil, false),
		CreateTestPlugin(&calls, 3, nil, nil, false),
	}
	err := target.Init(&pluginServiceImpl, props, plugins)
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
	defaultConnProvider := driver_infrastructure.ConnectionProvider(driver_infrastructure.NewDriverConnectionProvider(mockTargetDriver))
	target := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, &defaultConnProvider, nil, props)
	pluginServiceImpl := driver_infrastructure.PluginService(&plugin_helpers.PluginServiceImpl{})
	var calls []string

	plugins := []*driver_infrastructure.ConnectionPlugin{
		CreateTestPlugin(&calls, 1, nil, nil, false),
		CreateTestPlugin(&calls, 2, nil, nil, false),
		CreateTestPlugin(&calls, 3, nil, nil, false),
	}
	err := target.Init(&pluginServiceImpl, props, plugins)
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
	defaultConnProvider := driver_infrastructure.ConnectionProvider(driver_infrastructure.NewDriverConnectionProvider(mockTargetDriver))
	target := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, &defaultConnProvider, nil, props)
	pluginServiceImpl := driver_infrastructure.PluginService(&plugin_helpers.PluginServiceImpl{})
	var calls []string

	plugins := []*driver_infrastructure.ConnectionPlugin{
		CreateTestPlugin(&calls, 1, nil, nil, false),
		CreateTestPlugin(&calls, 2, nil, nil, false),
		CreateTestPlugin(&calls, 3, nil, nil, false),
	}
	err := target.Init(&pluginServiceImpl, props, plugins)
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
	defaultConnProvider := driver_infrastructure.ConnectionProvider(driver_infrastructure.NewDriverConnectionProvider(mockTargetDriver))
	target := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, &defaultConnProvider, nil, props)
	pluginServiceImpl := driver_infrastructure.PluginService(&plugin_helpers.PluginServiceImpl{})
	var calls []string
	expectedConn := MockDriverConnection{id: 123}

	plugins := []*driver_infrastructure.ConnectionPlugin{
		CreateTestPlugin(&calls, 1, nil, nil, false),
		CreateTestPlugin(&calls, 2, nil, nil, false),
		CreateTestPlugin(&calls, 3, expectedConn, nil, false),
	}
	err := target.Init(&pluginServiceImpl, props, plugins)
	if err != nil {
		return
	}

	_, _ = target.Connect(host_info_util.HostInfo{}, props, true)
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
	defaultConnProvider := driver_infrastructure.ConnectionProvider(driver_infrastructure.NewDriverConnectionProvider(mockTargetDriver))
	target := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, &defaultConnProvider, nil, props)
	pluginServiceImpl := driver_infrastructure.PluginService(&plugin_helpers.PluginServiceImpl{})
	var calls []string
	expectedConn := MockDriverConnection{id: 123}

	plugins := []*driver_infrastructure.ConnectionPlugin{
		CreateTestPlugin(&calls, 1, nil, nil, false),
		CreateTestPlugin(&calls, 2, nil, nil, false),
		CreateTestPlugin(&calls, 3, expectedConn, nil, false),
	}
	err := target.Init(&pluginServiceImpl, props, plugins)
	if err != nil {
		return
	}

	_, _ = target.ForceConnect(host_info_util.HostInfo{}, props, true)
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
	defaultConnProvider := driver_infrastructure.ConnectionProvider(driver_infrastructure.NewDriverConnectionProvider(mockTargetDriver))
	target := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, &defaultConnProvider, nil, props)
	pluginServiceImpl := driver_infrastructure.PluginService(&plugin_helpers.PluginServiceImpl{})
	var calls []string
	expectedConn := MockDriverConnection{id: 123}

	plugins := []*driver_infrastructure.ConnectionPlugin{
		CreateTestPlugin(&calls, 1, nil, nil, false),
		CreateTestPlugin(&calls, 2, nil, nil, false),
		CreateTestPlugin(&calls, 1, nil, expectedError, true),
		CreateTestPlugin(&calls, 3, expectedConn, nil, false),
	}

	err := target.Init(&pluginServiceImpl, props, plugins)
	if err != nil {
		return
	}

	_, _ = target.Connect(host_info_util.HostInfo{}, props, true)
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
	defaultConnProvider := driver_infrastructure.ConnectionProvider(driver_infrastructure.NewDriverConnectionProvider(mockTargetDriver))
	target := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, &defaultConnProvider, nil, props)
	pluginServiceImpl := driver_infrastructure.PluginService(&plugin_helpers.PluginServiceImpl{})
	var calls []string
	expectedConn := MockDriverConnection{id: 123}

	plugins := []*driver_infrastructure.ConnectionPlugin{
		CreateTestPlugin(&calls, 1, nil, nil, false),
		CreateTestPlugin(&calls, 2, nil, nil, false),
		CreateTestPlugin(&calls, 1, nil, expectedError, false),
		CreateTestPlugin(&calls, 3, expectedConn, nil, true),
	}
	err := target.Init(&pluginServiceImpl, props, plugins)
	if err != nil {
		return
	}

	_, _ = target.Connect(host_info_util.HostInfo{}, props, true)
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
	defaultConnProvider := driver_infrastructure.ConnectionProvider(driver_infrastructure.NewDriverConnectionProvider(mockTargetDriver))
	pluginServiceImpl := driver_infrastructure.PluginService(&plugin_helpers.PluginServiceImpl{})

	pluginManager1 := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, &defaultConnProvider, nil, props)
	pluginManager2 := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, &defaultConnProvider, nil, props)

	var calls []string
	plugins := []*driver_infrastructure.ConnectionPlugin{
		CreateTestPlugin(&calls, 1, nil, nil, false),
	}

	err := pluginManager1.Init(&pluginServiceImpl, props, plugins)
	if err != nil {
		t.Fatal(err)
	}
	err = pluginManager2.Init(&pluginServiceImpl, props, plugins)
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
