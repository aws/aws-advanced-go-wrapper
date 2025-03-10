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
	awsDriver "awssql/driver"
	"database/sql/driver"
)

type DefaultPlugin struct {
	pluginService awsDriver.PluginService
	// TODO: uncomment when needed.
	// pluginManager       awsDriver.PluginManager
	defaultConnProvider awsDriver.ConnectionProvider
	connProviderManager awsDriver.ConnectionProviderManager
}

func (d DefaultPlugin) InitHostProvider(initialUrl string, props map[string]any, hostListProviderService awsDriver.HostListProviderService, initHostProviderFunc func()) {
	// Do nothing.
	// It's guaranteed that this plugin is always the last in plugin chain so initHostProviderFunc can be omitted.
}

func (d DefaultPlugin) GetSubscribedMethods() []string {
	return []string{"*"}
}

func (d DefaultPlugin) Execute(methodName string, executeFunc awsDriver.ExecuteFunc, methodArgs ...any) (wrappedReturnValue any, wrappedReturnValue2 any, wrappedOk bool, wrappedErr error) {
	return executeFunc()
}

func (d DefaultPlugin) Connect(hostInfo awsDriver.HostInfo, properties map[string]any, isInitialConnection bool, connectFunc awsDriver.ConnectFunc) (*driver.Conn, error) {
	connProvider := d.connProviderManager.GetConnectionProvider(hostInfo, properties)
	return d.connectInternal(hostInfo, properties, connectFunc, connProvider)
}

func (d DefaultPlugin) ForceConnect(hostInfo awsDriver.HostInfo, properties map[string]any, isInitialConnection bool, connectFunc awsDriver.ConnectFunc) (*driver.Conn, error) {
	return d.connectInternal(hostInfo, properties, connectFunc, d.defaultConnProvider)
}

func (d DefaultPlugin) connectInternal(hostInfo awsDriver.HostInfo, properties map[string]any, connectFunc awsDriver.ConnectFunc, connProvider awsDriver.ConnectionProvider) (*driver.Conn, error) {
	conn, err := connProvider.Connect(hostInfo, properties)

	service := d.pluginService
	service.SetAvailability(hostInfo.AllAliases, awsDriver.AVAILABLE)

	return conn, err
}

func (d DefaultPlugin) AcceptsStrategy(role awsDriver.HostRole, strategy string) bool {
	//TODO implement me
	panic("implement me")
}

func (d DefaultPlugin) GetHostInfoByStrategy(role awsDriver.HostRole, strategy string, hosts []awsDriver.HostInfo) awsDriver.HostInfo {
	//TODO implement me
	panic("implement me")
}

func (d DefaultPlugin) NotifyConnectionChanged() awsDriver.OldConnectionSuggestedAction {
	return awsDriver.NO_OPINION
}

func (d DefaultPlugin) NotifyHostListChanged(changes map[awsDriver.HostChangeOptions]bool) {
	// Do nothing.
}
