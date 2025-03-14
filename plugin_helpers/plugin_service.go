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
	"database/sql/driver"
)

//nolint:unused
type PluginServiceImpl struct {
	// TODO: dialect should be initialized using DialectManager's GetDialect.
	pluginManager             *driver_infrastructure.PluginManager
	props                     map[string]string
	currentConnection         driver.Conn
	hostListProvider          driver_infrastructure.HostListProvider
	currentHostInfo           driver_infrastructure.HostInfo
	dialect                   driver_infrastructure.DatabaseDialect
	targetDriverDialect       driver_infrastructure.TargetDriverDialect
	dialectProvider           driver_infrastructure.DialectProvider
	connectionProviderManager driver_infrastructure.ConnectionProviderManager
	originalDsn               string
	hosts                     []driver_infrastructure.HostInfo
	allHosts                  []driver_infrastructure.HostInfo
	initialHost               string
	initialHostInfo           driver_infrastructure.HostInfo
	isInTransaction           bool
}

func NewPluginServiceImpl(pluginManager *driver_infrastructure.PluginManager, props map[string]string) *PluginServiceImpl {
	return &PluginServiceImpl{pluginManager: pluginManager, props: props}
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

func (p *PluginServiceImpl) GetCurrentConnection() driver.Conn {
	return p.currentConnection
}

func (p *PluginServiceImpl) SetCurrentConnection(
	conn driver.Conn,
	hostInfo driver_infrastructure.HostInfo,
	skipNotificationForThisPlugin driver_infrastructure.ConnectionPlugin) error {
	//TODO implement me
	panic("implement me")
}

func (p *PluginServiceImpl) GetCurrentHostInfo() driver_infrastructure.HostInfo {
	//TODO implement me
	panic("implement me")
}

func (p *PluginServiceImpl) GetHosts() []driver_infrastructure.HostInfo {
	return p.allHosts
}

func (p *PluginServiceImpl) GetInitialConnectionHostInfo() driver_infrastructure.HostInfo {
	return p.initialHostInfo
}

func (p *PluginServiceImpl) AcceptsStrategy(role driver_infrastructure.HostRole, strategy string) bool {
	return (*p.pluginManager).AcceptsStrategy(role, strategy)
}

func (p *PluginServiceImpl) GetHostInfoByStrategy(
	role driver_infrastructure.HostRole,
	strategy string,
	hosts []driver_infrastructure.HostInfo) (driver_infrastructure.HostInfo, error) {
	return (*p.pluginManager).GetHostInfoByStrategy(role, strategy, hosts)
}

func (p *PluginServiceImpl) GetHostRole(conn driver.Conn) driver_infrastructure.HostRole {
	return p.hostListProvider.GetHostRole(conn)
}

func (p *PluginServiceImpl) SetAvailability(hostAliases map[string]bool, availability driver_infrastructure.HostAvailability) {
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

func (p *PluginServiceImpl) UpdateHostAvailability(hosts []driver_infrastructure.HostInfo) error {
	//TODO implement me
	panic("implement me")
}

func (p *PluginServiceImpl) Connect(hostInfo driver_infrastructure.HostInfo, props map[string]string) (driver.Conn, error) {
	isInitialConnection := true
	if p.currentConnection != nil {
		isInitialConnection = false
	}
	return (*p.pluginManager).Connect(hostInfo, props, isInitialConnection)
}

func (p *PluginServiceImpl) ForceConnect(hostInfo driver_infrastructure.HostInfo, props map[string]string) (driver.Conn, error) {
	isInitialConnection := true
	if p.currentConnection != nil {
		isInitialConnection = false
	}
	return (*p.pluginManager).ForceConnect(hostInfo, props, isInitialConnection)
}

func (p *PluginServiceImpl) GetTargetDriverDialect() driver_infrastructure.TargetDriverDialect {
	//TODO implement me
	panic("implement me")
}

func (p *PluginServiceImpl) IdentifyConnection(conn driver.Conn) (driver_infrastructure.HostInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (p *PluginServiceImpl) FillAliases(conn driver.Conn, hostInfo driver_infrastructure.HostInfo) error {
	//TODO implement me
	panic("implement me")
}

func (p *PluginServiceImpl) GetHostInfoBuilder() driver_infrastructure.HostInfoBuilder {
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
