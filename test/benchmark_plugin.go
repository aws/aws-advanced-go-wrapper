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
	"awssql/driver_infrastructure"
	"awssql/host_info_util"
	"awssql/plugin_helpers"
	"awssql/plugins"
	"database/sql/driver"
)

type BenchmarkPluginFactory struct{}

func (b BenchmarkPluginFactory) GetInstance(pluginService driver_infrastructure.PluginService, props map[string]string) (driver_infrastructure.ConnectionPlugin, error) {
	return NewBenchmarkPlugin(pluginService, props), nil
}

type BenchmarkPlugin struct {
	plugins.BaseConnectionPlugin
	resources     []string
	pluginService driver_infrastructure.PluginService
	props         map[string]string
}

func NewBenchmarkPlugin(pluginService driver_infrastructure.PluginService, props map[string]string) *BenchmarkPlugin {
	return &BenchmarkPlugin{
		pluginService: pluginService,
		props:         props,
	}
}

func (b *BenchmarkPlugin) GetSubscribedMethods() []string {
	return []string{plugin_helpers.ALL_METHODS}
}

func (b *BenchmarkPlugin) Connect(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	b.resources = append(b.resources, "connect")
	return connectFunc()
}

func (b *BenchmarkPlugin) ForceConnect(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	b.resources = append(b.resources, "forceConnect")
	return connectFunc()
}

func (b *BenchmarkPlugin) Execute(
	methodName string,
	executeFunc driver_infrastructure.ExecuteFunc,
	methodArgs ...any) (wrappedReturnValue any, wrappedReturnValue2 any, wrappedOk bool, wrappedErr error) {
	b.resources = append(b.resources, "execute")
	return executeFunc()
}

func (b *BenchmarkPlugin) AcceptsStrategy(role host_info_util.HostRole, strategy string) bool {
	return false
}
func (b *BenchmarkPlugin) GetHostInfoByStrategy(role host_info_util.HostRole, strategy string, hosts []*host_info_util.HostInfo) (*host_info_util.HostInfo, error) {
	b.resources = append(b.resources, "getHostInfoByStrategy")
	return host_info_util.NewHostInfoBuilder().SetHost("host").SetPort(1234).SetRole(role).Build()
}

func (b *BenchmarkPlugin) NotifyConnectionChanged(changes map[driver_infrastructure.HostChangeOptions]bool) driver_infrastructure.OldConnectionSuggestedAction {
	return driver_infrastructure.NO_OPINION
}

func (b *BenchmarkPlugin) NotifyHostListChanged(changes map[string]map[driver_infrastructure.HostChangeOptions]bool) {
	b.resources = append(b.resources, "notifyHostListChanged")
}

func (b *BenchmarkPlugin) InitHostProvider(
	initialUrl string, props map[string]string,
	hostListProviderService driver_infrastructure.HostListProviderService,
	initHostProviderFunc func() error) error {
	b.resources = append(b.resources, "initHostProvider")
	return nil
}

func (b *BenchmarkPlugin) ReleaseResources() {
	b.resources = append(b.resources, "releaseResources")
}
