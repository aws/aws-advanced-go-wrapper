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
)

type DefaultPlugin struct {
	PluginService       PluginService
	DefaultConnProvider ConnectionProvider
	ConnProviderManager ConnectionProviderManager
}

func (d *DefaultPlugin) InitHostProvider(
	initialUrl string,
	props map[string]any,
	hostListProviderService HostListProviderService,
	initHostProviderFunc func() error) error {
	// Do nothing.
	// It's guaranteed that this plugin is always the last in plugin chain so initHostProviderFunc can be omitted.
	return nil
}

func (d *DefaultPlugin) GetSubscribedMethods() []string {
	return []string{"*"}
}

func (d *DefaultPlugin) Execute(methodName string, executeFunc ExecuteFunc, methodArgs ...any) (any, error) {
	return executeFunc()
}

func (d *DefaultPlugin) Connect(
	hostInfo HostInfo,
	properties map[string]any,
	isInitialConnection bool,
	connectFunc ConnectFunc) (driver.Conn, error) {
	// It's guaranteed that this plugin is always the last in plugin chain so connectFunc can be ignored.
	connProvider := d.ConnProviderManager.GetConnectionProvider(hostInfo, properties)
	return d.connectInternal(hostInfo, properties, connProvider)
}

func (d *DefaultPlugin) ForceConnect(
	hostInfo HostInfo,
	properties map[string]any,
	isInitialConnection bool,
	forceConnectFunc ConnectFunc) (driver.Conn, error) {
	// It's guaranteed that this plugin is always the last in plugin chain so connectFunc can be ignored.
	return d.connectInternal(hostInfo, properties, d.DefaultConnProvider)
}

func (d *DefaultPlugin) connectInternal(
	hostInfo HostInfo,
	properties map[string]any,
	connProvider ConnectionProvider) (driver.Conn, error) {
	conn, err := connProvider.Connect(hostInfo, properties)

	service := d.PluginService
	service.SetAvailability(hostInfo.AllAliases, AVAILABLE)

	return conn, err
}

func (d *DefaultPlugin) AcceptsStrategy(role HostRole, strategy string) bool {
	//TODO implement me
	panic("implement me")
}

func (d *DefaultPlugin) GetHostInfoByStrategy(role HostRole, strategy string, hosts []HostInfo) (HostInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (d *DefaultPlugin) NotifyConnectionChanged(changes map[HostChangeOptions]bool) OldConnectionSuggestedAction {
	return NO_OPINION
}

func (d *DefaultPlugin) NotifyHostListChanged(changes map[string]map[HostChangeOptions]bool) {
	// Do nothing.
}
