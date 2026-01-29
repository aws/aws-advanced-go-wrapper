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

package driver_infrastructure

import (
	"context"
	"database/sql/driver"

	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
)

type ConnectFunc func(props *utils.RWMap[string, string]) (driver.Conn, error)
type ExecuteFunc func() (any, any, bool, error)
type PluginExecFunc func(plugin ConnectionPlugin, targetFunc func() (any, any, bool, error)) (any, any, bool, error)
type PluginConnectFunc func(
	plugin ConnectionPlugin,
	props *utils.RWMap[string, string],
	targetFunc func(props *utils.RWMap[string, string]) (driver.Conn, error)) (driver.Conn, error)

type HostListProviderService interface {
	IsStaticHostListProvider() bool
	CreateHostListProvider(props *utils.RWMap[string, string]) HostListProvider
	GetHostListProvider() HostListProvider
	SetHostListProvider(hostListProvider HostListProvider)
	SetInitialConnectionHostInfo(info *host_info_util.HostInfo)
	GetDialect() DatabaseDialect
	GetCurrentConnection() driver.Conn
}

type PluginService interface {
	GetCurrentConnection() driver.Conn
	GetCurrentConnectionRef() *driver.Conn
	SetCurrentConnection(conn driver.Conn, hostInfo *host_info_util.HostInfo, skipNotificationForThisPlugin ConnectionPlugin) error
	GetInitialConnectionHostInfo() *host_info_util.HostInfo
	GetCurrentHostInfo() (*host_info_util.HostInfo, error)
	GetAllHosts() []*host_info_util.HostInfo
	GetHosts() []*host_info_util.HostInfo
	SetAllowedAndBlockedHosts(allowedAndBlockedHosts *AllowedAndBlockedHosts)
	AcceptsStrategy(strategy string) bool
	GetHostInfoByStrategy(role host_info_util.HostRole, strategy string, hosts []*host_info_util.HostInfo) (*host_info_util.HostInfo, error)
	GetHostSelectorStrategy(strategy string) (hostSelector HostSelector, err error)
	GetHostRole(driver.Conn) host_info_util.HostRole
	SetAvailability(hostAliases map[string]bool, availability host_info_util.HostAvailability)
	IsInTransaction() bool
	SetInTransaction(inTransaction bool)
	GetCurrentTx() driver.Tx
	SetCurrentTx(driver.Tx)
	CreateHostListProvider(props *utils.RWMap[string, string]) HostListProvider
	SetHostListProvider(hostListProvider HostListProvider)
	SetInitialConnectionHostInfo(info *host_info_util.HostInfo)
	IsStaticHostListProvider() bool
	GetHostListProvider() HostListProvider
	RefreshHostList(conn driver.Conn) error
	ForceRefreshHostList(conn driver.Conn) error
	ForceRefreshHostListWithTimeout(shouldVerifyWriter bool, timeoutMs int) (bool, error)
	GetUpdatedHostListWithTimeout(shouldVerifyWriter bool, timeoutMs int) ([]*host_info_util.HostInfo, error)
	Connect(hostInfo *host_info_util.HostInfo, props *utils.RWMap[string, string], pluginToSkip ConnectionPlugin) (driver.Conn, error)
	ForceConnect(hostInfo *host_info_util.HostInfo, props *utils.RWMap[string, string]) (driver.Conn, error)
	GetDialect() DatabaseDialect
	SetDialect(dialect DatabaseDialect)
	UpdateDialect(conn driver.Conn)
	GetTargetDriverDialect() DriverDialect
	IdentifyConnection(conn driver.Conn) (*host_info_util.HostInfo, error)
	FillAliases(conn driver.Conn, hostInfo *host_info_util.HostInfo)
	GetConnectionProvider() ConnectionProvider
	GetProperties() *utils.RWMap[string, string]
	IsNetworkError(err error) bool
	IsLoginError(err error) bool
	GetTelemetryContext() context.Context
	GetTelemetryFactory() telemetry.TelemetryFactory
	SetTelemetryContext(ctx context.Context)
	UpdateState(sql string, methodArgs ...any)
	GetBgStatus(id string) (BlueGreenStatus, bool)
	SetBgStatus(status BlueGreenStatus, id string)
	IsPluginInUse(pluginName string) bool
	ResetSession()
	CreatePartialPluginService() PluginService
}

type PluginServiceProvider func(
	pluginManager PluginManager,
	driverDialect DriverDialect,
	props *utils.RWMap[string, string],
	dsn string) (PluginService, error)

type PluginManager interface {
	Init(pluginService PluginService, plugins []ConnectionPlugin) error
	InitHostProvider(props *utils.RWMap[string, string], hostListProviderService HostListProviderService) error
	Connect(hostInfo *host_info_util.HostInfo, props *utils.RWMap[string, string], isInitialConnection bool, pluginToSkip ConnectionPlugin) (driver.Conn, error)
	ForceConnect(hostInfo *host_info_util.HostInfo, props *utils.RWMap[string, string], isInitialConnection bool) (driver.Conn, error)
	Execute(connInvokedOn driver.Conn, name string, methodFunc ExecuteFunc, methodArgs ...any) (
		wrappedReturnValue any,
		wrappedReturnValue2 any,
		wrappedOk bool,
		wrappedErr error)
	AcceptsStrategy(strategy string) bool
	NotifyHostListChanged(changes map[string]map[HostChangeOptions]bool)
	NotifyConnectionChanged(
		changes map[HostChangeOptions]bool, skipNotificationForThisPlugin ConnectionPlugin) map[OldConnectionSuggestedAction]bool
	NotifySubscribedPlugins(methodName string, pluginFunc PluginExecFunc, skipNotificationForThisPlugin ConnectionPlugin) error
	GetHostInfoByStrategy(role host_info_util.HostRole, strategy string, hosts []*host_info_util.HostInfo) (*host_info_util.HostInfo, error)
	GetHostSelectorStrategy(strategy string) (hostSelector HostSelector, err error)
	GetDefaultConnectionProvider() ConnectionProvider
	GetEffectiveConnectionProvider() ConnectionProvider
	GetConnectionProviderManager() ConnectionProviderManager
	GetTelemetryContext() context.Context
	GetTelemetryFactory() telemetry.TelemetryFactory
	SetTelemetryContext(ctx context.Context)
	IsPluginInUse(pluginName string) bool
	ReleaseResources()
	UnwrapPlugin(pluginCode string) ConnectionPlugin
}

type PluginManagerProvider func(
	targetDriver driver.Driver,
	props *utils.RWMap[string, string],
	connProviderManager ConnectionProviderManager,
	telemetryFactory telemetry.TelemetryFactory) PluginManager

type CanReleaseResources interface {
	ReleaseResources()
}

// This cleans up all long-standing caches. To be called at the end of program, not each time a Conn is closed.
func ClearCaches() {
	if knownEndpointDialectsCache != nil {
		knownEndpointDialectsCache.Clear()
	}
	if primaryClusterIdCache != nil {
		primaryClusterIdCache.Clear()
	}
	if suggestedPrimaryClusterCache != nil {
		suggestedPrimaryClusterCache.Clear()
	}
	if TopologyCache != nil {
		TopologyCache.Clear()
	}
}
