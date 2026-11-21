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

package plugins

import (
	"database/sql/driver"
	"errors"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils"
)

type connectionLifetimeAction int

const (
	connectionLifetimeReconnect connectionLifetimeAction = iota
	connectionLifetimeClose
)

var connectionLifetimeActionByName = map[string]connectionLifetimeAction{
	"reconnect": connectionLifetimeReconnect,
	"close":     connectionLifetimeClose,
}

// The lifetime is checked only before calls that reach the server, so an expired connection stays open until the
// next such call. Conn.IsValid is left out: database/sql calls it before returning a connection to the pool, and an
// action there would make the pool discard the connection, including a replacement that was just installed.
// Conn.Begin and Conn.BeginTx are added so an expired connection is handled before a transaction starts instead of
// after it ends.
var connectionLifetimeCheckedMethods = append(
	slices.DeleteFunc(slices.Clone(utils.NETWORK_BOUND_METHODS), func(method string) bool { return method == utils.CONN_IS_VALID }),
	utils.CONN_BEGIN,
	utils.CONN_BEGIN_TX)

type ConnectionLifetimePluginFactory struct{}

func NewConnectionLifetimePluginFactory() driver_infrastructure.ConnectionPluginFactory {
	return ConnectionLifetimePluginFactory{}
}

func (factory ConnectionLifetimePluginFactory) GetInstance(
	servicesContainer driver_infrastructure.ServicesContainer,
	props *utils.RWMap[string, string]) (driver_infrastructure.ConnectionPlugin, error) {
	return NewConnectionLifetimePlugin(servicesContainer, props)
}

func (factory ConnectionLifetimePluginFactory) ClearCaches() {}

// ConnectionLifetimePluginOption configures a ConnectionLifetimePlugin.
type ConnectionLifetimePluginOption func(*ConnectionLifetimePlugin)

// WithConnectionLifetimeClock replaces the clock used to measure connection age. The default is time.Now, whose
// monotonic reading keeps the measurement correct across wall-clock changes.
func WithConnectionLifetimeClock(now func() time.Time) ConnectionLifetimePluginOption {
	return func(plugin *ConnectionLifetimePlugin) {
		plugin.now = now
	}
}

// ConnectionLifetimePlugin limits how long a physical connection is used. Once the configured lifetime has passed,
// the next call that reaches the server either closes the connection or replaces it with a new connection to the
// host the application connected to. Either way the pending call is not executed and an error is returned.
type ConnectionLifetimePlugin struct {
	BaseConnectionPlugin
	servicesContainer driver_infrastructure.ServicesContainer
	pluginService     driver_infrastructure.PluginService
	props             *utils.RWMap[string, string]
	now               func() time.Time
	lifetime          time.Duration
	action            connectionLifetimeAction
	// Creation times keyed by physical connection, so switching back to a cached connection keeps its age.
	creationTimes      map[driver.Conn]time.Time
	originalHostInfo   *host_info_util.HostInfo
	timedConn          driver.Conn
	timedConnCreatedAt time.Time
	// Set once the plugin has closed the connection. Every later checked call fails.
	terminal bool
}

var _ driver_infrastructure.ConnectionPlugin = (*ConnectionLifetimePlugin)(nil)

func NewConnectionLifetimePlugin(
	servicesContainer driver_infrastructure.ServicesContainer,
	props *utils.RWMap[string, string],
	opts ...ConnectionLifetimePluginOption) (*ConnectionLifetimePlugin, error) {
	lifetime, err := getConnectionLifetime(props)
	if err != nil {
		return nil, err
	}
	action, err := getConnectionLifetimeAction(props)
	if err != nil {
		return nil, err
	}

	plugin := &ConnectionLifetimePlugin{
		servicesContainer: servicesContainer,
		pluginService:     servicesContainer.GetPluginService(),
		props:             props,
		now:               time.Now,
		lifetime:          lifetime,
		action:            action,
		creationTimes:     map[driver.Conn]time.Time{},
	}
	for _, opt := range opts {
		opt(plugin)
	}
	return plugin, nil
}

func getConnectionLifetime(props *utils.RWMap[string, string]) (time.Duration, error) {
	rawValue := property_util.CONNECTION_LIFETIME_MS.Get(props)
	lifetimeMs, err := strconv.ParseInt(strings.TrimSpace(rawValue), 10, 64)
	if err != nil || lifetimeMs < 1 {
		return 0, error_util.NewIllegalArgumentError(
			error_util.GetMessage("ConnectionLifetimePlugin.invalidConnectionLifetime", property_util.CONNECTION_LIFETIME_MS.Name, rawValue))
	}
	if lifetimeMs > int64(math.MaxInt64/time.Millisecond) {
		return time.Duration(math.MaxInt64), nil
	}
	return time.Duration(lifetimeMs) * time.Millisecond, nil
}

func getConnectionLifetimeAction(props *utils.RWMap[string, string]) (connectionLifetimeAction, error) {
	rawValue := property_util.CONNECTION_LIFETIME_ACTION.Get(props)
	action, ok := connectionLifetimeActionByName[strings.ToLower(strings.TrimSpace(rawValue))]
	if !ok {
		return 0, error_util.NewIllegalArgumentError(
			error_util.GetMessage("ConnectionLifetimePlugin.invalidConnectionLifetimeAction", property_util.CONNECTION_LIFETIME_ACTION.Name, rawValue))
	}
	return action, nil
}

func (p *ConnectionLifetimePlugin) GetPluginCode() string {
	return driver_infrastructure.CONNECTION_LIFETIME_PLUGIN_CODE
}

func (p *ConnectionLifetimePlugin) GetSubscribedMethods() []string {
	return append([]string{
		plugin_helpers.CONNECT_METHOD,
		plugin_helpers.NOTIFY_CONNECTION_CHANGED_METHOD,
	}, connectionLifetimeCheckedMethods...)
}

// Connect records when each physical connection is created. The host of the initial connection is kept as the
// reconnect target, because this plugin runs first and sees the host the application supplied before any routing,
// failover or initial connection strategy replaces it.
func (p *ConnectionLifetimePlugin) Connect(
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	if isInitialConnection && p.originalHostInfo == nil {
		p.originalHostInfo = hostInfo
	}
	conn, err := connectFunc(props)
	if err == nil && conn != nil {
		p.forgetClosedConnections()
		p.creationTimes[conn] = p.now()
	}
	return conn, err
}

func (p *ConnectionLifetimePlugin) NotifyConnectionChanged(
	changes map[driver_infrastructure.HostChangeOptions]bool) driver_infrastructure.OldConnectionSuggestedAction {
	if changes[driver_infrastructure.INITIAL_CONNECTION] || changes[driver_infrastructure.CONNECTION_OBJECT_CHANGED] {
		if current := p.pluginService.GetCurrentConnection(); current != nil {
			p.selectTimedConnection(current)
		}
	}
	return driver_infrastructure.NO_OPINION
}

func (p *ConnectionLifetimePlugin) Execute(
	_ driver.Conn,
	methodName string,
	executeFunc driver_infrastructure.ExecuteFunc,
	_ ...any) (wrappedReturnValue any, wrappedReturnValue2 any, wrappedOk bool, wrappedErr error) {
	if !slices.Contains(connectionLifetimeCheckedMethods, methodName) {
		return executeFunc()
	}
	if p.terminal {
		return nil, nil, false, error_util.NewConnectionLifetimeClosedError(nil)
	}
	if !p.isCurrentConnectionExpired() {
		return executeFunc()
	}
	// Deferring lets the transaction finish, including its commit or rollback. It does not reset the timer.
	if p.pluginService.IsInTransaction() {
		slog.Debug(error_util.GetMessage("ConnectionLifetimePlugin.actionDeferred", methodName))
		return executeFunc()
	}
	if p.action == connectionLifetimeClose {
		return nil, nil, false, p.closeCurrentConnection()
	}
	return nil, nil, false, p.reconnect()
}

// A connection this plugin did not see being created is timed from the first time it is noticed.
func (p *ConnectionLifetimePlugin) selectTimedConnection(conn driver.Conn) {
	createdAt, ok := p.creationTimes[conn]
	if !ok {
		createdAt = p.now()
		p.creationTimes[conn] = createdAt
	}
	p.timedConn = conn
	p.timedConnCreatedAt = createdAt
}

func (p *ConnectionLifetimePlugin) isCurrentConnectionExpired() bool {
	current := p.pluginService.GetCurrentConnection()
	if current == nil {
		return false
	}
	if current != p.timedConn {
		p.selectTimedConnection(current)
	}
	return p.now().Sub(p.timedConnCreatedAt) >= p.lifetime
}

// forgetClosedConnections drops the creation times of closed connections so the map does not grow with every
// connection opened over the life of the wrapper connection.
func (p *ConnectionLifetimePlugin) forgetClosedConnections() {
	current := p.pluginService.GetCurrentConnection()
	driverDialect := p.pluginService.GetTargetDriverDialect()
	for conn := range p.creationTimes {
		if conn != current && driverDialect.IsClosed(conn) {
			delete(p.creationTimes, conn)
		}
	}
}

func (p *ConnectionLifetimePlugin) closeCurrentConnection() error {
	p.terminal = true
	current := p.pluginService.GetCurrentConnection()
	delete(p.creationTimes, current)
	p.evictConnection(current)
	closeErr := current.Close()
	if closeErr != nil {
		slog.Warn(error_util.GetMessage("ConnectionLifetimePlugin.closeConnectionFailed", closeErr.Error()))
	}
	return error_util.NewConnectionLifetimeClosedError(closeErr)
}

func (p *ConnectionLifetimePlugin) reconnect() error {
	previous := p.pluginService.GetCurrentConnection()
	replacement, err := p.installReplacement()
	if err != nil {
		return error_util.NewConnectionLifetimeFailedError(p.cleanUpFailedReplacement(replacement, previous, err))
	}
	// Closing the expired connection returned a pooled one to its pool. A connection another plugin preserved is still
	// open and in use, so it stays.
	if previous != nil && previous != replacement && p.pluginService.GetTargetDriverDialect().IsClosed(previous) {
		p.evictConnection(previous)
	}
	p.forgetClosedConnections()
	return error_util.ConnectionLifetimeSuccessError
}

// installReplacement opens a connection to the original host and makes it the current connection. The timer is reset
// only after the plugin service has accepted the replacement. The replacement is returned with the error, if it was
// opened, so the caller can clean it up.
func (p *ConnectionLifetimePlugin) installReplacement() (driver.Conn, error) {
	hostInfo := p.originalHostInfo
	if hostInfo.IsNil() {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("ConnectionLifetimePlugin.originalHostUnavailable"))
	}

	slog.Debug(error_util.GetMessage("ConnectionLifetimePlugin.reconnecting", hostInfo.GetHostAndPort()))
	replacement, err := p.pluginService.Connect(hostInfo, p.props, p)
	if err != nil {
		return replacement, err
	}
	if replacement == nil {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("ConnectionLifetimePlugin.reconnectReturnedNil"))
	}
	createdAt := p.now()

	if err = p.pluginService.SetCurrentConnection(replacement, hostInfo, p); err != nil {
		return replacement, err
	}
	p.creationTimes[replacement] = createdAt
	p.timedConn = replacement
	p.timedConnCreatedAt = createdAt
	return replacement, nil
}

// cleanUpFailedReplacement closes what a failed reconnect left behind and returns reconnectErr joined with any close
// errors. A replacement that became current before installation failed leaves the plugin service in an unknown state,
// so both connections are closed and the plugin becomes terminal.
func (p *ConnectionLifetimePlugin) cleanUpFailedReplacement(replacement driver.Conn, previous driver.Conn, reconnectErr error) error {
	if replacement == nil {
		return reconnectErr
	}

	current := p.pluginService.GetCurrentConnection()
	if replacement == current && replacement != previous {
		p.terminal = true
		p.evictConnection(previous)
		return errors.Join(reconnectErr,
			p.closeAfterFailedReconnect(replacement, "ConnectionLifetimePlugin.failedInstallationConnectionCloseFailed"),
			p.closeAfterFailedReconnect(previous, "ConnectionLifetimePlugin.failedInstallationConnectionCloseFailed"))
	}
	if replacement == current {
		return reconnectErr
	}
	return errors.Join(reconnectErr, p.closeAfterFailedReconnect(replacement, "ConnectionLifetimePlugin.replacementCloseFailed"))
}

func (p *ConnectionLifetimePlugin) closeAfterFailedReconnect(conn driver.Conn, closeFailedMessageKey string) error {
	if conn == nil {
		return nil
	}
	delete(p.creationTimes, conn)
	err := conn.Close()
	if err != nil {
		slog.Warn(error_util.GetMessage(closeFailedMessageKey, err.Error()))
	}
	return err
}

// evictConnection keeps an expired connection from an internal pool from being handed out again, because closing it
// only returns it to the pool. Evicting before closing keeps the connection out of the idle list altogether.
func (p *ConnectionLifetimePlugin) evictConnection(conn driver.Conn) {
	if conn == nil {
		return
	}
	pluginManager := p.servicesContainer.GetPluginManager()
	if pluginManager == nil {
		return
	}
	cpm := pluginManager.GetConnectionProviderManager()
	cpm.EvictConnection(conn)
}
