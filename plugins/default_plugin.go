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
	"awssql/driver_infrastructure"
	"awssql/error_util"
	"awssql/host_info_util"
	"awssql/plugin_helpers"
	"awssql/utils"
	"database/sql/driver"
)

type DefaultPlugin struct {
	PluginService       driver_infrastructure.PluginService
	DefaultConnProvider driver_infrastructure.ConnectionProvider
	ConnProviderManager driver_infrastructure.ConnectionProviderManager
}

func (d *DefaultPlugin) InitHostProvider(
	initialUrl string,
	props map[string]string,
	hostListProviderService driver_infrastructure.HostListProviderService,
	initHostProviderFunc func() error) error {
	// Do nothing.
	// It's guaranteed that this plugin is always the last in plugin chain so initHostProviderFunc can be omitted.
	return nil
}

func (d *DefaultPlugin) GetSubscribedMethods() []string {
	return []string{plugin_helpers.ALL_METHODS}
}

func (d *DefaultPlugin) Execute(
	methodName string,
	executeFunc driver_infrastructure.ExecuteFunc,
	methodArgs ...any) (wrappedReturnValue any, wrappedReturnValue2 any, wrappedOk bool, wrappedErr error) {
	wrappedReturnValue, wrappedReturnValue2, wrappedOk, wrappedErr = executeFunc()
	if wrappedErr != nil {
		return
	}

	if utils.DoesOpenTransaction(methodName, methodArgs...) {
		d.PluginService.SetInTransaction(true)
	} else if utils.DoesCloseTransaction(methodName, methodArgs...) {
		d.PluginService.SetInTransaction(false)
	}

	return
}

func (d *DefaultPlugin) Connect(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	// It's guaranteed that this plugin is always the last in plugin chain so connectFunc can be ignored.
	connProvider := d.ConnProviderManager.GetConnectionProvider(*hostInfo, props)
	return d.connectInternal(hostInfo, props, connProvider, isInitialConnection)
}

func (d *DefaultPlugin) ForceConnect(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	isInitialConnection bool,
	forceConnectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	// It's guaranteed that this plugin is always the last in plugin chain so connectFunc can be ignored.
	return d.connectInternal(hostInfo, props, d.DefaultConnProvider, isInitialConnection)
}

func (d *DefaultPlugin) connectInternal(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	connProvider driver_infrastructure.ConnectionProvider,
	isInitialConnection bool) (driver.Conn, error) {
	conn, err := connProvider.Connect(hostInfo, props, d.PluginService)
	if err == nil {
		d.PluginService.SetAvailability(hostInfo.AllAliases, host_info_util.AVAILABLE)
		if isInitialConnection {
			d.PluginService.UpdateDialect(conn)
		}
	}
	return conn, err
}

func (d *DefaultPlugin) AcceptsStrategy(role host_info_util.HostRole, strategy string) bool {
	return d.ConnProviderManager.AcceptsStrategy(role, strategy)
}

func (d *DefaultPlugin) GetHostInfoByStrategy(
	role host_info_util.HostRole,
	strategy string,
	hosts []*host_info_util.HostInfo) (*host_info_util.HostInfo, error) {
	if len(hosts) == 0 {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("DefaultConnectionPlugin.noHostsAvailable"))
	}
	return d.ConnProviderManager.GetHostInfoByStrategy(hosts, role, strategy, d.PluginService.GetProperties())
}

func (d *DefaultPlugin) NotifyConnectionChanged(changes map[driver_infrastructure.HostChangeOptions]bool) driver_infrastructure.OldConnectionSuggestedAction {
	return driver_infrastructure.NO_OPINION
}

func (d *DefaultPlugin) NotifyHostListChanged(changes map[string]map[driver_infrastructure.HostChangeOptions]bool) {
	// Do nothing.
}
