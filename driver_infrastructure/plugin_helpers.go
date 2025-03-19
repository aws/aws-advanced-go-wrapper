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
	"awssql/host_info_util"
	"database/sql/driver"
)

type ConnectFunc func() (any, error)
type ExecuteFunc func() (any, any, bool, error)
type PluginExecFunc func(plugin ConnectionPlugin, targetFunc func() (any, any, bool, error)) (any, any, bool, error)
type PluginConnectFunc func(plugin ConnectionPlugin, targetFunc func() (any, error)) (any, error)

type PluginService interface {
	GetCurrentConnection() driver.Conn
	SetCurrentConnection(conn driver.Conn, hostInfo host_info_util.HostInfo, skipNotificationForThisPlugin ConnectionPlugin) error
	GetCurrentHostInfo() host_info_util.HostInfo
	GetHosts() []host_info_util.HostInfo
	GetInitialConnectionHostInfo() host_info_util.HostInfo
	AcceptsStrategy(role host_info_util.HostRole, strategy string) bool
	GetHostInfoByStrategy(role host_info_util.HostRole, strategy string, hosts []host_info_util.HostInfo) (host_info_util.HostInfo, error)
	GetHostRole(driver.Conn) host_info_util.HostRole
	SetAvailability(hostAliases map[string]bool, availability host_info_util.HostAvailability)
	InTransaction() bool
	GetHostListProvider() HostListProvider
	RefreshHostList(conn driver.Conn) error
	ForceRefreshHostList(conn driver.Conn) error // TODO: double check signatures, there are multiple
	Connect(hostInfo host_info_util.HostInfo, props map[string]string) (driver.Conn, error)
	ForceConnect(hostInfo host_info_util.HostInfo, props map[string]string) (driver.Conn, error)
	GetDialect() DatabaseDialect
	UpdateDialect(conn driver.Conn)
	GetTargetDriverDialect() DriverDialect
	IdentifyConnection(conn driver.Conn) (host_info_util.HostInfo, error)
	FillAliases(conn driver.Conn, hostInfo host_info_util.HostInfo) error
	GetHostInfoBuilder() host_info_util.HostInfoBuilder
	GetConnectionProvider() ConnectionProvider
	GetProperties() map[string]string
	IsNetworkError(err error) bool
	IsLoginError(err error) bool
}

type PluginManager interface {
	Init(pluginService *PluginService, props map[string]string, plugins []*ConnectionPlugin) error
	InitHostProvider(initialUrl string, props map[string]string, hostListProviderService HostListProviderService) error
	Connect(hostInfo host_info_util.HostInfo, props map[string]string, isInitialConnection bool) (driver.Conn, error)
	ForceConnect(hostInfo host_info_util.HostInfo, props map[string]string, isInitialConnection bool) (driver.Conn, error)
	Execute(name string, methodFunc ExecuteFunc, methodArgs ...any) (
		wrappedReturnValue any,
		wrappedReturnValue2 any,
		wrappedOk bool,
		wrappedErr error)
	AcceptsStrategy(role host_info_util.HostRole, strategy string) bool
	NotifyHostListChanged(changes map[string]map[HostChangeOptions]bool)
	NotifyConnectionChanged(
		changes map[HostChangeOptions]bool, skipNotificationForThisPlugin ConnectionPlugin) map[OldConnectionSuggestedAction]bool
	NotifySubscribedPlugins(methodName string, pluginFunc PluginExecFunc, skipNotificationForThisPlugin ConnectionPlugin) error
	GetHostInfoByStrategy(role host_info_util.HostRole, strategy string, hosts []host_info_util.HostInfo) (host_info_util.HostInfo, error)
	GetDefaultConnectionProvider() *ConnectionProvider
	GetEffectiveConnectionProvider() *ConnectionProvider
	GetConnectionProviderManager() ConnectionProviderManager
}
