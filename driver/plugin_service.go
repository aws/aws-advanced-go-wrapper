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

import "database/sql/driver"

type PluginService interface {
	GetCurrentConnection() driver.Conn
	SetCurrentConnection(conn driver.Conn, hostInfo HostInfo, skipNotificationForThisPlugin ConnectionPlugin) error
	GetCurrentHostInfo() HostInfo
	GetHosts() []HostInfo
	GetInitialConnectionHostInfo() HostInfo
	AcceptsStrategy(role HostRole, strategy string) bool
	GetHostInfoByStrategy(role HostRole, strategy string, hosts []HostInfo) (HostInfo, error)
	GetHostRole(driver.Conn) HostRole
	SetAvailability(hostAliases map[string]bool, availability HostAvailability)
	InTransaction() bool
	GetHostListProvider() HostListProvider
	RefreshHostList(conn driver.Conn) error
	ForceRefreshHostList(conn driver.Conn) error // TODO: double check signatures, there are multiple
	Connect(hostInfo HostInfo, props map[string]any) (driver.Conn, error)
	ForceConnect(hostInfo HostInfo, props map[string]any) (driver.Conn, error)
	GetDialect() DatabaseDialect
	UpdateDialect(conn driver.Conn)
	GetTargetDriverDialect() TargetDriverDialect
	IdentifyConnection(conn driver.Conn) (HostInfo, error)
	FillAliases(conn driver.Conn, hostInfo HostInfo) error
	GetHostInfoBuilder() HostInfoBuilder
	GetConnectionProvider() ConnectionProvider
	GetProperties() map[string]any
}

type PluginServiceImpl struct {
	// TODO: dialect should be initialized using DialectManager's GetDialect.
	dialect                   DatabaseDialect
	dialectProvider           DialectProvider
	initialHost               string
	props                     map[string]any
	pluginManager             PluginManager
	currentConnection         driver.Conn
	hostListProvider          HostListProvider
	currentHostInfo           HostInfo
	targetDriverDialect       TargetDriverDialect
	connectionProviderManager ConnectionProviderManager
	originalDsn               string
	hosts                     []HostInfo
	allHosts                  []HostInfo
	initialHostInfo           HostInfo
	isInTransaction           bool
}

func NewPluginServiceImpl(pluginManager PluginManager, props map[string]any) *PluginServiceImpl {
	return &PluginServiceImpl{pluginManager: pluginManager, props: props}
}

func (p *PluginServiceImpl) GetDialect() DatabaseDialect {
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
	hostInfo HostInfo,
	skipNotificationForThisPlugin ConnectionPlugin) error {
	//TODO implement me
	panic("implement me")
}

func (p *PluginServiceImpl) GetCurrentHostInfo() HostInfo {
	//TODO implement me
	panic("implement me")
}

func (p *PluginServiceImpl) GetHosts() []HostInfo {
	return p.allHosts
}

func (p *PluginServiceImpl) GetInitialConnectionHostInfo() HostInfo {
	return p.initialHostInfo
}

func (p *PluginServiceImpl) AcceptsStrategy(role HostRole, strategy string) bool {
	return p.pluginManager.AcceptsStrategy(role, strategy)
}

func (p *PluginServiceImpl) GetHostInfoByStrategy(role HostRole, strategy string, hosts []HostInfo) (HostInfo, error) {
	return p.pluginManager.GetHostInfoByStrategy(role, strategy, hosts)
}

func (p *PluginServiceImpl) GetHostRole(conn driver.Conn) HostRole {
	return p.hostListProvider.GetHostRole(conn)
}

func (p *PluginServiceImpl) SetAvailability(hostAliases map[string]bool, availability HostAvailability) {
	//TODO implement me
	panic("implement me")
}

func (p *PluginServiceImpl) InTransaction() bool {
	return p.isInTransaction
}

func (p *PluginServiceImpl) GetHostListProvider() HostListProvider {
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

func (p *PluginServiceImpl) UpdateHostAvailability(hosts []HostInfo) error {
	//TODO implement me
	panic("implement me")
}

func (p *PluginServiceImpl) Connect(hostInfo HostInfo, props map[string]any) (driver.Conn, error) {
	isInitialConnection := true
	if p.currentConnection != nil {
		isInitialConnection = false
	}
	return p.pluginManager.Connect(hostInfo, props, isInitialConnection)
}

func (p *PluginServiceImpl) ForceConnect(hostInfo HostInfo, props map[string]any) (driver.Conn, error) {
	isInitialConnection := true
	if p.currentConnection != nil {
		isInitialConnection = false
	}
	return p.pluginManager.ForceConnect(hostInfo, props, isInitialConnection)
}

func (p *PluginServiceImpl) GetTargetDriverDialect() TargetDriverDialect {
	//TODO implement me
	panic("implement me")
}

func (p *PluginServiceImpl) IdentifyConnection(conn driver.Conn) (HostInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (p *PluginServiceImpl) FillAliases(conn driver.Conn, hostInfo HostInfo) error {
	//TODO implement me
	panic("implement me")
}

func (p *PluginServiceImpl) GetHostInfoBuilder() HostInfoBuilder {
	//TODO implement me
	panic("implement me")
}

func (p *PluginServiceImpl) GetConnectionProvider() ConnectionProvider {
	//TODO implement me
	panic("implement me")
}

func (p *PluginServiceImpl) GetProperties() map[string]any {
	return p.props
}
