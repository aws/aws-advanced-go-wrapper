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

package plugin_helpers

import (
	"awssql/driver_infrastructure"
	"awssql/error_util"
	"awssql/host_info_util"
	"database/sql/driver"
	"log"
)

//nolint:unused
type PluginServiceImpl struct {
	// TODO: dialect should be initialized using DialectManager's GetDialect.
	pluginManager             *driver_infrastructure.PluginManager
	props                     map[string]string
	currentConnection         driver.Conn
	hostListProvider          driver_infrastructure.HostListProvider
	currentHostInfo           host_info_util.HostInfo
	dialect                   driver_infrastructure.DatabaseDialect
	driverDialect             driver_infrastructure.DriverDialect
	dialectProvider           driver_infrastructure.DialectProvider
	connectionProviderManager driver_infrastructure.ConnectionProviderManager
	originalDsn               string
	hosts                     []host_info_util.HostInfo
	allHosts                  []host_info_util.HostInfo
	initialHost               string
	initialHostInfo           host_info_util.HostInfo
	isInTransaction           bool
}

func NewPluginServiceImpl(pluginManager *driver_infrastructure.PluginManager,
	driverDialect driver_infrastructure.DriverDialect,
	props map[string]string,
	dsn string) *PluginServiceImpl {
	pluginService := &PluginServiceImpl{pluginManager: pluginManager, driverDialect: driverDialect, props: props}
	dialectProvider := driver_infrastructure.DialectManager{}
	dialect, err := dialectProvider.GetDialect(dsn, props)
	if err == nil {
		hostListProvider := dialect.GetHostListProvider(props, dsn, pluginService)
		pluginService.dialectProvider = &dialectProvider
		pluginService.dialect = dialect
		pluginService.hostListProvider = *hostListProvider
	}
	return pluginService
}

func (p *PluginServiceImpl) IsStaticHostListProvider() bool {
	return p.hostListProvider.IsStaticHostListProvider()
}

func (p *PluginServiceImpl) SetHostListProvider(hostListProvider driver_infrastructure.HostListProvider) {
	p.hostListProvider = hostListProvider
}

func (p *PluginServiceImpl) SetInitialConnectionHostInfo(hostInfo host_info_util.HostInfo) {
	p.initialHostInfo = hostInfo
}

func (p *PluginServiceImpl) GetDialect() driver_infrastructure.DatabaseDialect {
	return p.dialect
}

func (p *PluginServiceImpl) UpdateDialect(conn driver.Conn) {
	// TODO: provide information on newHost and initialHost to dialectProvider.
	newDialect := p.dialectProvider.GetDialectForUpdate(conn, p.initialHost, "")
	if p.dialect == newDialect {
		return
	}
	p.dialect = newDialect
	// TODO: update HostListProvider based on new dialect.
}

func (p *PluginServiceImpl) GetCurrentConnection() *driver.Conn {
	return &p.currentConnection
}

func (p *PluginServiceImpl) SetCurrentConnection(
	conn driver.Conn,
	hostInfo host_info_util.HostInfo,
	skipNotificationForThisPlugin driver_infrastructure.ConnectionPlugin) error {
	//TODO implement me
	panic("implement me")
}

func (p *PluginServiceImpl) GetCurrentHostInfo() host_info_util.HostInfo {
	//TODO implement me
	panic("implement me")
}

func (p *PluginServiceImpl) GetHosts() []host_info_util.HostInfo {
	return p.allHosts
}

func (p *PluginServiceImpl) GetInitialConnectionHostInfo() host_info_util.HostInfo {
	return p.initialHostInfo
}

func (p *PluginServiceImpl) AcceptsStrategy(role host_info_util.HostRole, strategy string) bool {
	return (*p.pluginManager).AcceptsStrategy(role, strategy)
}

func (p *PluginServiceImpl) GetHostInfoByStrategy(
	role host_info_util.HostRole,
	strategy string,
	hosts []host_info_util.HostInfo) (host_info_util.HostInfo, error) {
	return (*p.pluginManager).GetHostInfoByStrategy(role, strategy, hosts)
}

func (p *PluginServiceImpl) GetHostRole(conn driver.Conn) host_info_util.HostRole {
	return p.hostListProvider.GetHostRole(conn)
}

func (p *PluginServiceImpl) SetAvailability(hostAliases map[string]bool, availability host_info_util.HostAvailability) {
	//TODO implement me
	panic("implement me")
}

func (p *PluginServiceImpl) InTransaction() bool {
	return p.isInTransaction
}

func (p *PluginServiceImpl) GetHostListProvider() driver_infrastructure.HostListProvider {
	return p.hostListProvider
}

func (p *PluginServiceImpl) RefreshHostList(conn driver.Conn) error {
	//TODO implement me
	panic("implement me")
}

func (p *PluginServiceImpl) ForceRefreshHostList(conn driver.Conn) error {
	//TODO implement me
	panic("implement me")
}

func (p *PluginServiceImpl) UpdateHostAvailability(hosts []host_info_util.HostInfo) error {
	//TODO implement me
	panic("implement me")
}

func (p *PluginServiceImpl) Connect(hostInfo host_info_util.HostInfo, props map[string]string) (driver.Conn, error) {
	isInitialConnection := true
	if p.currentConnection != nil {
		isInitialConnection = false
	}
	return (*p.pluginManager).Connect(hostInfo, props, isInitialConnection)
}

func (p *PluginServiceImpl) ForceConnect(hostInfo host_info_util.HostInfo, props map[string]string) (driver.Conn, error) {
	isInitialConnection := true
	if p.currentConnection != nil {
		isInitialConnection = false
	}
	return (*p.pluginManager).ForceConnect(hostInfo, props, isInitialConnection)
}

func (p *PluginServiceImpl) GetTargetDriverDialect() driver_infrastructure.DriverDialect {
	return p.driverDialect
}

func (p *PluginServiceImpl) IdentifyConnection(conn driver.Conn) (host_info_util.HostInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (p *PluginServiceImpl) FillAliases(conn driver.Conn, hostInfo host_info_util.HostInfo) error {
	//TODO implement me
	panic("implement me")
}

func (p *PluginServiceImpl) GetConnectionProvider() driver_infrastructure.ConnectionProvider {
	//TODO implement me
	panic("implement me")
}

func (p *PluginServiceImpl) GetProperties() map[string]string {
	return p.props
}

func (p *PluginServiceImpl) IsNetworkError(err error) bool {
	return p.driverDialect.IsNetworkError(err)
}

func (p *PluginServiceImpl) IsLoginError(err error) bool {
	return p.driverDialect.IsLoginError(err)
}

func (p *PluginServiceImpl) ReleaseResources() {
	log.Println(error_util.GetMessage("PluginServiceImpl.releaseResources"))

	if p.currentConnection != nil {
		p.currentConnection.Close() // Ignore any error.
		p.currentConnection = nil
	}

	if p.hostListProvider != nil {
		canReleaseResources, ok := p.hostListProvider.(driver_infrastructure.CanReleaseResources)

		if ok {
			canReleaseResources.ReleaseResources()
		}
	}
}
