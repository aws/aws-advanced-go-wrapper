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

package driver

import (
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

func (t TestPlugin) Execute(methodName string, executeFunc ExecuteFunc, methodArgs ...any) (any, error) {
	*t.calls = append(*t.calls, fmt.Sprintf("%s%v:before", reflect.TypeOf(t), t.id))
	if t.isBefore && t.error != nil {
		return nil, t.error
	}
	result, err := executeFunc()
	*t.calls = append(*t.calls, fmt.Sprintf("%s%v:after", reflect.TypeOf(t), t.id))
	if !t.isBefore && t.error != nil {
		return nil, t.error
	}
	return result, err
}

func (t TestPlugin) Connect(
	hostInfo HostInfo,
	properties map[string]any,
	isInitialConnection bool,
	connectFunc ConnectFunc) (driver.Conn, error) {
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
	hostInfo HostInfo,
	properties map[string]any,
	isInitialConnection bool,
	connectFunc ConnectFunc) (driver.Conn, error) {
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

func (t TestPlugin) AcceptsStrategy(role HostRole, strategy string) bool {
	return false
}

func (t TestPlugin) GetHostInfoByStrategy(role HostRole, strategy string, hosts []HostInfo) (HostInfo, error) {
	*t.calls = append(*t.calls, fmt.Sprintf("%s%v:before GetHostInfoByStrategy", reflect.TypeOf(t), t.id))
	result := HostInfo{}
	*t.calls = append(*t.calls, fmt.Sprintf("%s%v:after GetHostInfoByStrategy", reflect.TypeOf(t), t.id))
	return result, nil
}

func (t TestPlugin) NotifyConnectionChanged(changes map[HostChangeOptions]bool) OldConnectionSuggestedAction {
	return NO_OPINION
}

func (t TestPlugin) NotifyHostListChanged(changes map[string]map[HostChangeOptions]bool) {
	// Do nothing
}

func (t TestPlugin) InitHostProvider(
	initialUrl string,
	props map[string]any,
	hostListProviderService HostListProviderService,
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
	//TODO implement me
	panic("implement me")
}

func (m MockDriverConnection) Close() error {
	//TODO implement me
	panic("implement me")
}

func (m MockDriverConnection) Begin() (driver.Tx, error) {
	//TODO implement me
	panic("implement me")
}

func TestExecuteFunctionCallA(t *testing.T) {
	mockTargetDriver := &MockTargetDriver{}
	props := make(map[string]any)
	target := NewPluginManager(mockTargetDriver, DriverConnectionProvider{targetDriver: mockTargetDriver}, nil, props)
	var calls []string
	plugins := []ConnectionPlugin{
		&TestPlugin{calls: &calls, id: 1},
		&TestPlugin{calls: &calls, id: 2},
		&TestPlugin{calls: &calls, id: 3}}
	err := target.Init(&PluginServiceImpl{}, props, plugins)
	if err != nil {
		return
	}

	execFunc := func() (any, error) {
		calls = append(calls, "targetCall")
		return "resultTestValue", nil
	}

	result, _ := target.Execute("callA", execFunc, 10, "arg2, 3.33")
	if result != "resultTestValue" {
		t.Fatalf(`Result should be "resultTestValue", got "%s"`, result)
	}
	if len(calls) != 7 {
		t.Fatalf(`Should have 7 calls, got "%v"`, len(calls))
	}
	if calls[0] != "driver.TestPlugin1:before" ||
		calls[1] != "driver.TestPlugin2:before" ||
		calls[2] != "driver.TestPlugin3:before" ||
		calls[3] != "targetCall" ||
		calls[4] != "driver.TestPlugin3:after" ||
		calls[5] != "driver.TestPlugin2:after" ||
		calls[6] != "driver.TestPlugin1:after" {
		t.Fatalf(`Incorrect calls, got "%v"`, calls)
	}
}

func TestExecuteFunctionCallB(t *testing.T) {
	mockTargetDriver := &MockTargetDriver{}
	props := make(map[string]any)
	target := NewPluginManager(mockTargetDriver, DriverConnectionProvider{targetDriver: mockTargetDriver}, nil, props)
	var calls []string
	plugins := []ConnectionPlugin{
		&TestPlugin{calls: &calls, id: 1},
		&TestPlugin{calls: &calls, id: 2},
		&TestPlugin{calls: &calls, id: 3}}
	err := target.Init(&PluginServiceImpl{}, props, plugins)
	if err != nil {
		return
	}

	execFunc := func() (any, error) {
		calls = append(calls, "targetCall")
		return "resultTestValue", nil
	}

	result, _ := target.Execute("callB", execFunc, 10, "arg2, 3.33")
	if result != "resultTestValue" {
		t.Fatalf(`Result should be "resultTestValue", got "%s"`, result)
	}
	if len(calls) != 5 {
		t.Fatalf(`Should have 5 calls, got "%v"`, len(calls))
	}
	if calls[0] != "driver.TestPlugin1:before" ||
		calls[1] != "driver.TestPlugin2:before" ||
		calls[2] != "targetCall" ||
		calls[3] != "driver.TestPlugin2:after" ||
		calls[4] != "driver.TestPlugin1:after" {
		t.Fatalf(`Incorrect calls, got "%v"`, calls)
	}
}

func TestExecuteFunctionCallC(t *testing.T) {
	mockTargetDriver := &MockTargetDriver{}
	props := make(map[string]any)
	target := NewPluginManager(mockTargetDriver, DriverConnectionProvider{targetDriver: mockTargetDriver}, nil, props)
	var calls []string
	plugins := []ConnectionPlugin{
		&TestPlugin{calls: &calls, id: 1},
		&TestPlugin{calls: &calls, id: 2},
		&TestPlugin{calls: &calls, id: 3}}
	err := target.Init(&PluginServiceImpl{}, props, plugins)
	if err != nil {
		return
	}

	execFunc := func() (any, error) {
		calls = append(calls, "targetCall")
		return "resultTestValue", nil
	}

	result, _ := target.Execute("callC", execFunc, 10, "arg2, 3.33")
	if result != "resultTestValue" {
		t.Fatalf(`Result should be "resultTestValue", got "%s"`, result)
	}
	if len(calls) != 3 {
		t.Fatalf(`Should have 3 calls, got "%v"`, len(calls))
	}
	if calls[0] != "driver.TestPlugin1:before" ||
		calls[1] != "targetCall" ||
		calls[2] != "driver.TestPlugin1:after" {
		t.Fatalf(`Incorrect calls, got "%v"`, calls)
	}
}

func TestConnect(t *testing.T) {
	mockTargetDriver := &MockTargetDriver{}
	props := make(map[string]any)
	target := NewPluginManager(mockTargetDriver, DriverConnectionProvider{targetDriver: mockTargetDriver}, nil, props)
	var calls []string
	expectedConn := MockDriverConnection{id: 123}
	plugins := []ConnectionPlugin{
		&TestPlugin{calls: &calls, id: 1},
		&TestPlugin{calls: &calls, id: 2},
		&TestPlugin{calls: &calls, id: 3, connection: expectedConn}}
	err := target.Init(&PluginServiceImpl{}, props, plugins)
	if err != nil {
		return
	}

	_, _ = target.Connect(HostInfo{}, props, true)
	if len(calls) != 4 {
		t.Fatalf(`Should have 4 calls, got "%v"`, len(calls))
	}
	if calls[0] != "driver.TestPlugin1:before connect" ||
		calls[1] != "driver.TestPlugin3:before connect" ||
		calls[2] != "driver.TestPlugin3:connection" ||
		calls[3] != "driver.TestPlugin1:after connect" {
		t.Fatalf(`Incorrect calls, got "%v"`, calls)
	}
}

func TestForceConnect(t *testing.T) {
	mockTargetDriver := &MockTargetDriver{}
	props := make(map[string]any)
	target := NewPluginManager(mockTargetDriver, DriverConnectionProvider{targetDriver: mockTargetDriver}, nil, props)
	var calls []string
	expectedConn := MockDriverConnection{id: 123}
	plugins := []ConnectionPlugin{
		&TestPlugin{calls: &calls, id: 1},
		&TestPlugin{calls: &calls, id: 2},
		&TestPlugin{calls: &calls, id: 3, connection: expectedConn}}
	err := target.Init(&PluginServiceImpl{}, props, plugins)
	if err != nil {
		return
	}

	_, _ = target.ForceConnect(HostInfo{}, props, true)
	if len(calls) != 4 {
		t.Fatalf(`Should have 4 calls, got "%v"`, len(calls))
	}
	if calls[0] != "driver.TestPlugin1:before forceConnect" ||
		calls[1] != "driver.TestPlugin3:before forceConnect" ||
		calls[2] != "driver.TestPlugin3:forced connection" ||
		calls[3] != "driver.TestPlugin1:after forceConnect" {
		t.Fatalf(`Incorrect calls, got "%v"`, calls)
	}
}

func TestConnectWithErrorBefore(t *testing.T) {
	expectedError := errors.New("test error")
	mockTargetDriver := &MockTargetDriver{}
	props := make(map[string]any)
	target := NewPluginManager(mockTargetDriver, DriverConnectionProvider{targetDriver: mockTargetDriver}, nil, props)
	var calls []string
	expectedConn := MockDriverConnection{id: 123}
	plugins := []ConnectionPlugin{
		&TestPlugin{calls: &calls, id: 1},
		&TestPlugin{calls: &calls, id: 2},
		&TestPlugin{calls: &calls, id: 1, error: expectedError, isBefore: true},
		&TestPlugin{calls: &calls, id: 3, connection: expectedConn}}
	err := target.Init(&PluginServiceImpl{}, props, plugins)
	if err != nil {
		return
	}

	_, _ = target.Connect(HostInfo{}, props, true)
	if len(calls) != 3 {
		t.Fatalf(`Should have 3 calls, got "%v"`, len(calls))
	}
	if calls[0] != "driver.TestPlugin1:before connect" ||
		calls[1] != "driver.TestPlugin1:before connect" ||
		calls[2] != "driver.TestPlugin1:after connect" {
		t.Fatalf(`Incorrect calls, got "%v"`, calls)
	}
}

func TestConnectWithErrorAfter(t *testing.T) {
	expectedError := errors.New("test error")
	mockTargetDriver := &MockTargetDriver{}
	props := make(map[string]any)
	target := NewPluginManager(mockTargetDriver, DriverConnectionProvider{targetDriver: mockTargetDriver}, nil, props)
	var calls []string
	expectedConn := MockDriverConnection{id: 123}
	plugins := []ConnectionPlugin{
		&TestPlugin{calls: &calls, id: 1},
		&TestPlugin{calls: &calls, id: 2},
		&TestPlugin{calls: &calls, id: 1, error: expectedError, isBefore: false},
		&TestPlugin{calls: &calls, id: 3, connection: expectedConn}}
	err := target.Init(&PluginServiceImpl{}, props, plugins)
	if err != nil {
		return
	}

	_, _ = target.Connect(HostInfo{}, props, true)
	if len(calls) != 5 {
		t.Fatalf(`Should have 5 calls, got "%v"`, len(calls))
	}
	if calls[0] != "driver.TestPlugin1:before connect" ||
		calls[1] != "driver.TestPlugin1:before connect" ||
		calls[2] != "driver.TestPlugin3:before connect" ||
		calls[3] != "driver.TestPlugin3:connection" ||
		calls[4] != "driver.TestPlugin1:after connect" {
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
	props := make(map[string]any)
	pluginManager1 := NewPluginManager(mockTargetDriver, DriverConnectionProvider{targetDriver: mockTargetDriver}, nil, props)
	err := pluginManager1.Init(&PluginServiceImpl{}, props, nil)
	if err != nil {
		return
	}

	dbResourceMutex := &sync.Mutex{}
	dbResourceReleased := atomic.Bool{}
	acquireDbResourceMutexSuccessful := atomic.Bool{}

	group1 := func(wg *sync.WaitGroup) {
		_, _ = executeWithPlugins(*pluginManager1, "lock-thread-1",
			func() (any, error) {
				dbResourceMutex.Lock()
				waitForDbResourceLocked.Done()
				return 1, nil
			},
		)
		waitForReleaseDbResourceToProceed.Wait()
		_, _ = executeWithPlugins(*pluginManager1, "release-thread-1",
			func() (any, error) {
				dbResourceMutex.Unlock()
				dbResourceReleased.Store(true)
				return 1, nil
			},
		)
		routinesWg.Done()
	}

	group2 := func(wg *sync.WaitGroup) {
		waitForDbResourceLocked.Wait()
		_, _ = executeWithPlugins(*pluginManager1, "lock-thread-2",
			func() (any, error) {
				waitForReleaseDbResourceToProceed.Done()
				time.Sleep(3 * time.Second)
				tryLock := dbResourceMutex.TryLock()
				acquireDbResourceMutexSuccessful.Store(tryLock)
				return 1, nil
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
