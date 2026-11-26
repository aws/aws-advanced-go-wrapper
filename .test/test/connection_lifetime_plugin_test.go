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
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/plugins"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type connectionLifetimeFixture struct {
	t             *testing.T
	ctrl          *gomock.Controller
	pluginService *mock_driver_infrastructure.MockPluginService
	container     *mock_driver_infrastructure.MockServicesContainer
	base          time.Time
	elapsed       time.Duration
	current       driver.Conn
	inTransaction bool
	closed        map[driver.Conn]bool
	originalHost  *host_info_util.HostInfo
	movedHost     *host_info_util.HostInfo
	// Calls in order: "close" from a connection set up with expectClose, "evict" from the connection provider.
	events  []string
	evicted []driver.Conn
}

// evictingConnectionProvider records the connections the plugin evicts. Its other methods are never called.
type evictingConnectionProvider struct {
	driver_infrastructure.ConnectionProvider
	f *connectionLifetimeFixture
}

func (p *evictingConnectionProvider) EvictConnection(conn driver.Conn) bool {
	p.f.events = append(p.f.events, "evict")
	p.f.evicted = append(p.f.evicted, conn)
	return true
}

func newConnectionLifetimeFixture(t *testing.T) *connectionLifetimeFixture {
	ctrl := gomock.NewController(t)
	f := &connectionLifetimeFixture{
		t:             t,
		ctrl:          ctrl,
		pluginService: mock_driver_infrastructure.NewMockPluginService(ctrl),
		container:     mock_driver_infrastructure.NewMockServicesContainer(ctrl),
		base:          time.Now(),
		closed:        map[driver.Conn]bool{},
		originalHost:  &host_info_util.HostInfo{Host: "original.example.com", Port: 5432},
		movedHost:     &host_info_util.HostInfo{Host: "moved.example.com", Port: 5432},
	}
	driverDialect := mock_driver_infrastructure.NewMockDriverDialect(ctrl)
	driverDialect.EXPECT().IsClosed(gomock.Any()).DoAndReturn(func(conn driver.Conn) bool { return f.closed[conn] }).AnyTimes()
	f.container.EXPECT().GetPluginService().Return(f.pluginService).AnyTimes()
	pluginManager := mock_driver_infrastructure.NewMockPluginManager(ctrl)
	f.container.EXPECT().GetPluginManager().Return(pluginManager).AnyTimes()
	pluginManager.EXPECT().GetConnectionProviderManager().Return(driver_infrastructure.ConnectionProviderManager{
		DefaultProvider: &evictingConnectionProvider{f: f},
	}).AnyTimes()
	f.pluginService.EXPECT().GetTargetDriverDialect().Return(driverDialect).AnyTimes()
	f.pluginService.EXPECT().GetCurrentConnection().DoAndReturn(func() driver.Conn { return f.current }).AnyTimes()
	f.pluginService.EXPECT().IsInTransaction().DoAndReturn(func() bool { return f.inTransaction }).AnyTimes()
	return f
}

func (f *connectionLifetimeFixture) setMillis(ms int) {
	f.elapsed = time.Duration(ms) * time.Millisecond
}

func (f *connectionLifetimeFixture) now() time.Time {
	return f.base.Add(f.elapsed)
}

func (f *connectionLifetimeFixture) newPlugin(lifetimeMs string, action string) *plugins.ConnectionLifetimePlugin {
	props := MakeMapFromKeysAndVals(property_util.CONNECTION_LIFETIME_MS.Name, lifetimeMs)
	if action != "" {
		props.Put(property_util.CONNECTION_LIFETIME_ACTION.Name, action)
	}
	plugin, err := plugins.NewConnectionLifetimePlugin(f.container, props, plugins.WithConnectionLifetimeClock(f.now))
	require.NoError(f.t, err)
	return plugin
}

func (f *connectionLifetimeFixture) newConn() *mock_database_sql_driver.MockConn {
	return mock_database_sql_driver.NewMockConn(f.ctrl)
}

// initializeConnection opens the initial connection through the plugin and installs it the way driver.Open does.
func (f *connectionLifetimeFixture) initializeConnection(plugin *plugins.ConnectionLifetimePlugin, conn driver.Conn, atMs int) {
	f.setMillis(atMs)
	opened, err := plugin.Connect(f.originalHost, utils.NewRWMap[string, string](), true, returnConn(conn))
	require.NoError(f.t, err)
	f.current = opened
	plugin.NotifyConnectionChanged(map[driver_infrastructure.HostChangeOptions]bool{driver_infrastructure.INITIAL_CONNECTION: true})
}

func (f *connectionLifetimeFixture) switchTo(plugin *plugins.ConnectionLifetimePlugin, conn driver.Conn) {
	f.current = conn
	plugin.NotifyConnectionChanged(map[driver_infrastructure.HostChangeOptions]bool{driver_infrastructure.CONNECTION_OBJECT_CHANGED: true})
}

// expectReconnect makes the plugin service return replacement for a reconnect to the original host and install it.
func (f *connectionLifetimeFixture) expectReconnect(plugin *plugins.ConnectionLifetimePlugin, replacement driver.Conn) *gomock.Call {
	f.pluginService.EXPECT().SetCurrentConnection(replacement, f.originalHost, gomock.Any()).
		DoAndReturn(func(conn driver.Conn, _ *host_info_util.HostInfo, pluginToSkip driver_infrastructure.ConnectionPlugin) error {
			assert.Same(f.t, plugin, pluginToSkip)
			f.current = conn
			return nil
		})
	return f.pluginService.EXPECT().Connect(f.originalHost, gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ *host_info_util.HostInfo, _ *utils.RWMap[string, string], pluginToSkip driver_infrastructure.ConnectionPlugin) (driver.Conn, error) {
			assert.Same(f.t, plugin, pluginToSkip)
			return replacement, nil
		})
}

// expectClose expects one Close on conn and records it in f.events.
func (f *connectionLifetimeFixture) expectClose(conn *mock_database_sql_driver.MockConn, err error) {
	conn.EXPECT().Close().DoAndReturn(func() error {
		f.events = append(f.events, "close")
		f.closed[conn] = true
		return err
	})
}

func returnConn(conn driver.Conn) driver_infrastructure.ConnectFunc {
	return func(_ *utils.RWMap[string, string]) (driver.Conn, error) { return conn, nil }
}

// executeLifetime runs methodName through the plugin and reports whether the pending call ran.
func executeLifetime(plugin *plugins.ConnectionLifetimePlugin, methodName string) (bool, error) {
	called := false
	_, _, _, err := plugin.Execute(nil, methodName, func() (any, any, bool, error) {
		called = true
		return nil, nil, true, nil
	})
	return called, err
}

func TestConnectionLifetimePlugin_MissingLifetimeFails(t *testing.T) {
	f := newConnectionLifetimeFixture(t)

	plugin, err := plugins.NewConnectionLifetimePlugin(f.container, utils.NewRWMap[string, string]())

	assert.Nil(t, plugin)
	assert.True(t, error_util.IsType(err, error_util.IllegalArgumentErrorType))
	assert.Equal(t, error_util.GetMessage("ConnectionLifetimePlugin.invalidConnectionLifetime", property_util.CONNECTION_LIFETIME_MS.Name, ""), err.Error())
}

func TestConnectionLifetimePlugin_InvalidLifetimeFails(t *testing.T) {
	for _, value := range []string{"0", "-1", "abc", "1.5", ""} {
		t.Run(value, func(t *testing.T) {
			f := newConnectionLifetimeFixture(t)
			props := MakeMapFromKeysAndVals(property_util.CONNECTION_LIFETIME_MS.Name, value)

			_, err := plugins.NewConnectionLifetimePlugin(f.container, props)

			assert.True(t, error_util.IsType(err, error_util.IllegalArgumentErrorType))
		})
	}
}

func TestConnectionLifetimePlugin_InvalidActionFails(t *testing.T) {
	for _, action := range []string{"", "drop", "close-now"} {
		t.Run(action, func(t *testing.T) {
			f := newConnectionLifetimeFixture(t)
			props := MakeMapFromKeysAndVals(
				property_util.CONNECTION_LIFETIME_MS.Name, "100",
				property_util.CONNECTION_LIFETIME_ACTION.Name, action)

			_, err := plugins.NewConnectionLifetimePlugin(f.container, props)

			assert.True(t, error_util.IsType(err, error_util.IllegalArgumentErrorType))
			assert.Equal(t,
				error_util.GetMessage("ConnectionLifetimePlugin.invalidConnectionLifetimeAction", property_util.CONNECTION_LIFETIME_ACTION.Name, action),
				err.Error())
		})
	}
}

func TestConnectionLifetimePlugin_FactoryValidatesConfiguration(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	factory := plugins.NewConnectionLifetimePluginFactory()

	plugin, err := factory.GetInstance(f.container, MakeMapFromKeysAndVals(property_util.CONNECTION_LIFETIME_MS.Name, " 300000 "))
	require.NoError(t, err)
	assert.Equal(t, driver_infrastructure.CONNECTION_LIFETIME_PLUGIN_CODE, plugin.GetPluginCode())

	_, err = factory.GetInstance(f.container, utils.NewRWMap[string, string]())
	assert.Error(t, err)
}

func TestConnectionLifetimePlugin_SubscribesOnlyToNetworkAndLifecycleHooks(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	subscribed := f.newPlugin("100", "close").GetSubscribedMethods()

	for _, method := range []string{
		plugin_helpers.CONNECT_METHOD, plugin_helpers.NOTIFY_CONNECTION_CHANGED_METHOD,
		utils.CONN_QUERY_CONTEXT, utils.CONN_EXEC_CONTEXT, utils.CONN_PREPARE_CONTEXT, utils.CONN_PING, utils.CONN_RESET_SESSION,
		utils.CONN_BEGIN, utils.CONN_BEGIN_TX, utils.STMT_EXEC_CONTEXT, utils.TX_COMMIT, utils.TX_ROLLBACK, utils.ROWS_NEXT,
	} {
		assert.Contains(t, subscribed, method)
	}
	for _, method := range []string{
		plugin_helpers.FORCE_CONNECT_METHOD, plugin_helpers.INIT_HOST_PROVIDER_METHOD, plugin_helpers.ALL_METHODS,
		utils.CONN_CLOSE, utils.CONN_IS_VALID, utils.STMT_CLOSE, utils.ROWS_CLOSE, utils.STMT_NUM_INPUT, utils.CONN_CHECK_NAMED_VALUE,
	} {
		assert.NotContains(t, subscribed, method)
	}
}

func TestConnectionLifetimePlugin_CallBeforeExpirationRunsAndBoundaryIsInclusive(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	plugin := f.newPlugin("100", "close")
	conn := f.newConn()
	f.initializeConnection(plugin, conn, 0)

	f.setMillis(99)
	called, err := executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)
	assert.NoError(t, err)
	assert.True(t, called)

	conn.EXPECT().Close().Return(nil)
	f.setMillis(100)
	called, err = executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)
	assert.False(t, called)
	assert.True(t, error_util.IsType(err, error_util.ConnectionLifetimeClosedErrorType))
}

func TestConnectionLifetimePlugin_CloseActionClosesBeforeFailingAndStaysTerminal(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	plugin := f.newPlugin("100", "close")
	conn := f.newConn()
	f.initializeConnection(plugin, conn, 0)
	f.expectClose(conn, nil)
	f.setMillis(100)

	called, err := executeLifetime(plugin, utils.STMT_EXEC_CONTEXT)
	assert.False(t, called)
	assert.True(t, error_util.IsType(err, error_util.ConnectionLifetimeClosedErrorType))
	assert.ErrorIs(t, err, driver.ErrBadConn)
	assert.Equal(t, error_util.GetMessage("ConnectionLifetimePlugin.connectionClosed"), err.Error())
	// Evicting first keeps a pooled connection out of the idle list, where another caller could take it.
	assert.Equal(t, []string{"evict", "close"}, f.events)
	assert.Equal(t, []driver.Conn{conn}, f.evicted)

	// Later calls fail the same way without closing again, even once the clock no longer says expired.
	f.setMillis(0)
	called, err = executeLifetime(plugin, utils.CONN_PING)
	assert.False(t, called)
	assert.True(t, error_util.IsType(err, error_util.ConnectionLifetimeClosedErrorType))
	assert.ErrorIs(t, err, driver.ErrBadConn)
	assert.Len(t, f.evicted, 1)
}

func TestConnectionLifetimePlugin_CloseActionKeepsCloseErrorAsCause(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	plugin := f.newPlugin("100", "close")
	conn := f.newConn()
	f.initializeConnection(plugin, conn, 0)
	closeErr := errors.New("close failed")
	conn.EXPECT().Close().Return(closeErr)
	f.setMillis(100)

	_, err := executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)

	assert.True(t, error_util.IsType(err, error_util.ConnectionLifetimeClosedErrorType))
	assert.ErrorIs(t, err, closeErr)
	assert.ErrorIs(t, err, driver.ErrBadConn)
}

func TestConnectionLifetimePlugin_ActionIsCaseInsensitiveAndTrimmed(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	plugin := f.newPlugin("100", " CLOSE ")
	conn := f.newConn()
	f.initializeConnection(plugin, conn, 0)
	conn.EXPECT().Close().Return(nil)
	f.setMillis(100)

	_, err := executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)

	assert.True(t, error_util.IsType(err, error_util.ConnectionLifetimeClosedErrorType))
}

func TestConnectionLifetimePlugin_UncheckedMethodsIgnoreExpirationAndTerminalState(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	plugin := f.newPlugin("100", "close")
	conn := f.newConn()
	f.initializeConnection(plugin, conn, 0)
	f.setMillis(500)

	// No Close expectation: an action here would fail the mock.
	for _, method := range []string{utils.CONN_CLOSE, utils.CONN_IS_VALID, utils.STMT_CLOSE, utils.ROWS_CLOSE, utils.STMT_NUM_INPUT} {
		called, err := executeLifetime(plugin, method)
		assert.NoError(t, err, method)
		assert.True(t, called, method)
	}

	conn.EXPECT().Close().Return(nil)
	_, err := executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)
	require.Error(t, err)

	called, err := executeLifetime(plugin, utils.CONN_CLOSE)
	assert.NoError(t, err)
	assert.True(t, called)
}

func TestConnectionLifetimePlugin_BeginIsChecked(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	plugin := f.newPlugin("100", "close")
	conn := f.newConn()
	f.initializeConnection(plugin, conn, 0)
	conn.EXPECT().Close().Return(nil)
	f.setMillis(100)

	called, err := executeLifetime(plugin, utils.CONN_BEGIN_TX)

	assert.False(t, called)
	assert.True(t, error_util.IsType(err, error_util.ConnectionLifetimeClosedErrorType))
}

func TestConnectionLifetimePlugin_ConnectTimestampIsKeptThroughInitialNotification(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	plugin := f.newPlugin("100", "close")
	conn := f.newConn()
	f.initializeConnection(plugin, conn, 0)
	f.setMillis(50)
	plugin.NotifyConnectionChanged(map[driver_infrastructure.HostChangeOptions]bool{driver_infrastructure.INITIAL_CONNECTION: true})
	conn.EXPECT().Close().Return(nil)
	f.setMillis(100)

	_, err := executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)

	assert.Error(t, err)
}

func TestConnectionLifetimePlugin_HostOnlyNotificationDoesNotResetTimestamp(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	plugin := f.newPlugin("100", "close")
	conn := f.newConn()
	f.initializeConnection(plugin, conn, 0)
	f.setMillis(90)
	plugin.NotifyConnectionChanged(map[driver_infrastructure.HostChangeOptions]bool{
		driver_infrastructure.HOSTNAME:     true,
		driver_infrastructure.HOST_CHANGED: true,
	})
	conn.EXPECT().Close().Return(nil)
	f.setMillis(100)

	_, err := executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)

	assert.Error(t, err)
}

func TestConnectionLifetimePlugin_ConnectionSwitchedByAnotherPluginUsesItsCreationTime(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	plugin := f.newPlugin("100", "close")
	first := f.newConn()
	second := f.newConn()
	f.initializeConnection(plugin, first, 0)

	// Another plugin opens a connection at 50 and switches to it at 80.
	f.setMillis(50)
	_, err := plugin.Connect(f.movedHost, utils.NewRWMap[string, string](), false, returnConn(second))
	require.NoError(t, err)
	f.setMillis(80)
	f.switchTo(plugin, second)

	f.setMillis(149)
	called, err := executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)
	assert.NoError(t, err)
	assert.True(t, called)

	second.EXPECT().Close().Return(nil)
	f.setMillis(150)
	called, err = executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)
	assert.False(t, called)
	assert.Error(t, err)
}

func TestConnectionLifetimePlugin_SwitchingBackToCachedConnectionKeepsItsCreationTime(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	plugin := f.newPlugin("100", "close")
	first := f.newConn()
	second := f.newConn()
	f.initializeConnection(plugin, first, 0)
	f.setMillis(50)
	_, err := plugin.Connect(f.movedHost, utils.NewRWMap[string, string](), false, returnConn(second))
	require.NoError(t, err)
	f.switchTo(plugin, second)
	f.setMillis(99)
	f.switchTo(plugin, first)

	first.EXPECT().Close().Return(nil)
	f.setMillis(100)
	_, err = executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)

	assert.Error(t, err)
}

func TestConnectionLifetimePlugin_ConnectionNotSeenByConnectIsTimedFromFirstNotice(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	plugin := f.newPlugin("100", "close")
	conn := f.newConn()
	f.current = conn

	f.setMillis(50)
	called, err := executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)
	assert.NoError(t, err)
	assert.True(t, called)

	f.setMillis(149)
	called, err = executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)
	assert.NoError(t, err)
	assert.True(t, called)

	conn.EXPECT().Close().Return(nil)
	f.setMillis(150)
	_, err = executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)
	assert.Error(t, err)
}

func TestConnectionLifetimePlugin_DefaultReconnectUsesOriginalHostAndSkipsPendingCall(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	plugin := f.newPlugin("100", "")
	original := f.newConn()
	replacement := f.newConn()
	f.initializeConnection(plugin, original, 0)
	f.expectReconnect(plugin, replacement)
	f.setMillis(100)

	called, err := executeLifetime(plugin, utils.CONN_EXEC_CONTEXT)

	assert.False(t, called)
	assert.ErrorIs(t, err, error_util.ConnectionLifetimeSuccessError)
	assert.True(t, error_util.IsType(err, error_util.FailoverSuccessErrorType))
	assert.NotErrorIs(t, err, error_util.FailoverSuccessError)
	assert.NotErrorIs(t, err, driver.ErrBadConn)
	assert.Same(t, replacement, f.current)
}

func TestConnectionLifetimePlugin_ReconnectIgnoresHostsChosenAfterTheInitialConnect(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	plugin := f.newPlugin("100", "reconnect")
	routed := f.newConn()
	replacement := f.newConn()

	// The outer initial connect carries the application's host; a nested initial connect, as failover or the
	// initial connection strategy would make, targets a different host and must not replace it.
	f.setMillis(0)
	_, err := plugin.Connect(f.originalHost, utils.NewRWMap[string, string](), true,
		func(props *utils.RWMap[string, string]) (driver.Conn, error) {
			return plugin.Connect(f.movedHost, props, true, returnConn(routed))
		})
	require.NoError(t, err)
	f.current = routed
	plugin.NotifyConnectionChanged(map[driver_infrastructure.HostChangeOptions]bool{driver_infrastructure.INITIAL_CONNECTION: true})

	f.expectReconnect(plugin, replacement)
	f.setMillis(100)
	_, err = executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)

	assert.ErrorIs(t, err, error_util.ConnectionLifetimeSuccessError)
}

func TestConnectionLifetimePlugin_ReconnectIsCaseInsensitiveAndResetsTimerAfterSuccess(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	plugin := f.newPlugin("100", "ReCoNnEcT")
	original := f.newConn()
	first := f.newConn()
	second := f.newConn()
	f.initializeConnection(plugin, original, 0)
	gomock.InOrder(f.expectReconnect(plugin, first), f.expectReconnect(plugin, second))

	f.setMillis(100)
	_, err := executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)
	assert.ErrorIs(t, err, error_util.ConnectionLifetimeSuccessError)

	f.setMillis(199)
	called, err := executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)
	assert.NoError(t, err)
	assert.True(t, called)

	f.setMillis(200)
	_, err = executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)
	assert.ErrorIs(t, err, error_util.ConnectionLifetimeSuccessError)
	assert.Same(t, second, f.current)
}

func TestConnectionLifetimePlugin_ReconnectFailureKeepsCauseAndDoesNotResetTimer(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	plugin := f.newPlugin("100", "reconnect")
	original := f.newConn()
	f.initializeConnection(plugin, original, 0)
	connectErr := errors.New("connect failed")
	f.pluginService.EXPECT().Connect(f.originalHost, gomock.Any(), gomock.Any()).Return(nil, connectErr).Times(2)

	f.setMillis(100)
	called, err := executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)
	assert.False(t, called)
	assert.True(t, error_util.IsType(err, error_util.ConnectionLifetimeFailedErrorType))
	assert.ErrorIs(t, err, connectErr)
	assert.ErrorIs(t, err, driver.ErrBadConn)
	assert.Same(t, original, f.current)

	// The timer was not reset, so the next checked call tries again.
	f.setMillis(101)
	_, err = executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)
	assert.True(t, error_util.IsType(err, error_util.ConnectionLifetimeFailedErrorType))
}

func TestConnectionLifetimePlugin_InstallationFailureClosesUnattachedReplacement(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	plugin := f.newPlugin("100", "reconnect")
	original := f.newConn()
	replacement := f.newConn()
	f.initializeConnection(plugin, original, 0)
	installErr := errors.New("install failed")
	f.pluginService.EXPECT().Connect(f.originalHost, gomock.Any(), gomock.Any()).Return(replacement, nil)
	f.pluginService.EXPECT().SetCurrentConnection(replacement, f.originalHost, gomock.Any()).Return(installErr)
	replacement.EXPECT().Close().Return(nil)

	f.setMillis(100)
	_, err := executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)

	assert.True(t, error_util.IsType(err, error_util.ConnectionLifetimeFailedErrorType))
	assert.ErrorIs(t, err, installErr)
	assert.Same(t, original, f.current)
	assert.Empty(t, f.evicted)

	// The original connection keeps its timer and the plugin is not terminal.
	failedAgain := errors.New("connect failed")
	f.pluginService.EXPECT().Connect(f.originalHost, gomock.Any(), gomock.Any()).Return(nil, failedAgain)
	f.setMillis(150)
	_, err = executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)
	assert.ErrorIs(t, err, failedAgain)
}

func TestConnectionLifetimePlugin_PartialInstallationClosesBothConnectionsAndIsTerminal(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	plugin := f.newPlugin("100", "reconnect")
	original := f.newConn()
	replacement := f.newConn()
	f.initializeConnection(plugin, original, 0)
	installErr := errors.New("session state failed")
	replacementCloseErr := errors.New("replacement close failed")
	f.pluginService.EXPECT().Connect(f.originalHost, gomock.Any(), gomock.Any()).Return(replacement, nil)
	f.pluginService.EXPECT().SetCurrentConnection(replacement, f.originalHost, gomock.Any()).
		DoAndReturn(func(conn driver.Conn, _ *host_info_util.HostInfo, _ driver_infrastructure.ConnectionPlugin) error {
			f.current = conn
			return installErr
		})
	replacement.EXPECT().Close().Return(replacementCloseErr)
	original.EXPECT().Close().Return(nil)

	f.setMillis(100)
	_, err := executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)
	assert.True(t, error_util.IsType(err, error_util.ConnectionLifetimeFailedErrorType))
	assert.ErrorIs(t, err, installErr)
	assert.ErrorIs(t, err, replacementCloseErr)
	// Only the expired connection is evicted; the new replacement may go back to its pool.
	assert.Equal(t, []driver.Conn{original}, f.evicted)

	called, err := executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)
	assert.False(t, called)
	assert.True(t, error_util.IsType(err, error_util.ConnectionLifetimeClosedErrorType))
}

func TestConnectionLifetimePlugin_NilReplacementIsReconnectFailure(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	plugin := f.newPlugin("100", "reconnect")
	original := f.newConn()
	f.initializeConnection(plugin, original, 0)
	f.pluginService.EXPECT().Connect(f.originalHost, gomock.Any(), gomock.Any()).Return(nil, nil)

	f.setMillis(100)
	_, err := executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)

	assert.True(t, error_util.IsType(err, error_util.ConnectionLifetimeFailedErrorType))
	assert.Contains(t, err.Error(), error_util.GetMessage("ConnectionLifetimePlugin.reconnectReturnedNil"))
}

func TestConnectionLifetimePlugin_ReconnectWithoutOriginalHostFails(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	plugin := f.newPlugin("100", "reconnect")
	f.current = f.newConn()

	f.setMillis(0)
	_, err := executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)
	require.NoError(t, err)
	f.setMillis(100)
	_, err = executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)

	assert.True(t, error_util.IsType(err, error_util.ConnectionLifetimeFailedErrorType))
	assert.Contains(t, err.Error(), error_util.GetMessage("ConnectionLifetimePlugin.originalHostUnavailable"))
}

func TestConnectionLifetimePlugin_TransactionDefersReconnectThroughCommit(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	plugin := f.newPlugin("100", "reconnect")
	original := f.newConn()
	replacement := f.newConn()
	f.initializeConnection(plugin, original, 0)
	f.inTransaction = true

	f.setMillis(100)
	for _, method := range []string{utils.CONN_EXEC_CONTEXT, utils.TX_COMMIT} {
		called, err := executeLifetime(plugin, method)
		assert.NoError(t, err, method)
		assert.True(t, called, method)
	}

	f.inTransaction = false
	f.expectReconnect(plugin, replacement)
	called, err := executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)
	assert.False(t, called)
	assert.ErrorIs(t, err, error_util.ConnectionLifetimeSuccessError)
}

func TestConnectionLifetimePlugin_TransactionDefersCloseThroughRollback(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	plugin := f.newPlugin("100", "close")
	conn := f.newConn()
	f.initializeConnection(plugin, conn, 0)
	f.inTransaction = true

	f.setMillis(150)
	for _, method := range []string{utils.CONN_QUERY_CONTEXT, utils.TX_ROLLBACK} {
		called, err := executeLifetime(plugin, method)
		assert.NoError(t, err, method)
		assert.True(t, called, method)
	}

	f.inTransaction = false
	conn.EXPECT().Close().Return(nil)
	called, err := executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)
	assert.False(t, called)
	assert.True(t, error_util.IsType(err, error_util.ConnectionLifetimeClosedErrorType))
}

func TestConnectionLifetimePlugin_ClosedReplacedConnectionIsForgotten(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	plugin := f.newPlugin("100", "reconnect")
	original := f.newConn()
	replacement := f.newConn()
	f.initializeConnection(plugin, original, 0)
	f.expectReconnect(plugin, replacement)
	f.closed[original] = true

	f.setMillis(100)
	_, err := executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)
	require.ErrorIs(t, err, error_util.ConnectionLifetimeSuccessError)
	// The plugin service closed it, which would return a pooled connection to its pool.
	assert.Equal(t, []driver.Conn{original}, f.evicted)

	// Its creation time was dropped, so if it ever becomes current again it is timed from that point.
	f.setMillis(150)
	f.switchTo(plugin, original)
	called, err := executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)
	assert.NoError(t, err)
	assert.True(t, called)
}

func TestConnectionLifetimePlugin_OpenReplacedConnectionKeepsItsCreationTime(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	plugin := f.newPlugin("100", "reconnect")
	original := f.newConn()
	first := f.newConn()
	second := f.newConn()
	f.initializeConnection(plugin, original, 0)
	gomock.InOrder(f.expectReconnect(plugin, first), f.expectReconnect(plugin, second))

	f.setMillis(100)
	_, err := executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)
	require.ErrorIs(t, err, error_util.ConnectionLifetimeSuccessError)
	// Another plugin kept it open for reuse, so it stays in its pool.
	assert.Empty(t, f.evicted)

	// The original connection was preserved and is still open, so it is still expired when switched back to.
	f.setMillis(150)
	f.switchTo(plugin, original)
	called, err := executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)
	assert.False(t, called)
	assert.ErrorIs(t, err, error_util.ConnectionLifetimeSuccessError)
}

func TestConnectionLifetimePlugin_ClosedConnectionsFromConnectAreForgotten(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	plugin := f.newPlugin("100", "close")
	current := f.newConn()
	discarded := f.newConn()
	later := f.newConn()
	f.initializeConnection(plugin, current, 0)

	// Another plugin opens a connection at 10 and closes it without using it.
	f.setMillis(10)
	_, err := plugin.Connect(f.movedHost, utils.NewRWMap[string, string](), false, returnConn(discarded))
	require.NoError(t, err)
	f.closed[discarded] = true

	// The next connect drops it, so it is timed afresh if it becomes current.
	f.setMillis(20)
	_, err = plugin.Connect(f.movedHost, utils.NewRWMap[string, string](), false, returnConn(later))
	require.NoError(t, err)
	f.setMillis(110)
	f.switchTo(plugin, discarded)

	called, err := executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)
	assert.NoError(t, err)
	assert.True(t, called)
}

func TestConnectionLifetimePlugin_SuccessiveReconnectsEvictEachClosedConnection(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	plugin := f.newPlugin("100", "reconnect")
	original := f.newConn()
	first := f.newConn()
	second := f.newConn()
	f.initializeConnection(plugin, original, 0)
	gomock.InOrder(f.expectReconnect(plugin, first), f.expectReconnect(plugin, second))

	f.closed[original] = true
	f.setMillis(100)
	_, err := executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)
	require.ErrorIs(t, err, error_util.ConnectionLifetimeSuccessError)

	f.closed[first] = true
	f.setMillis(200)
	_, err = executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)
	require.ErrorIs(t, err, error_util.ConnectionLifetimeSuccessError)

	assert.Equal(t, []driver.Conn{original, first}, f.evicted)
}

func TestConnectionLifetimePlugin_ReconnectFailureDoesNotEvict(t *testing.T) {
	f := newConnectionLifetimeFixture(t)
	plugin := f.newPlugin("100", "reconnect")
	f.initializeConnection(plugin, f.newConn(), 0)
	f.pluginService.EXPECT().Connect(f.originalHost, gomock.Any(), gomock.Any()).Return(nil, errors.New("connect failed"))

	f.setMillis(100)
	_, err := executeLifetime(plugin, utils.CONN_QUERY_CONTEXT)

	assert.True(t, error_util.IsType(err, error_util.ConnectionLifetimeFailedErrorType))
	assert.Empty(t, f.evicted)
}
