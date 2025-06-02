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
	"awssql/container"
	"awssql/driver_infrastructure"
	"awssql/host_info_util"
	"awssql/plugin_helpers"
	"awssql/property_util"
	"testing"
)

var BENCHMARK_DEFAULT_NUM_PLUGINS int = 10

func initPluginManagerWithPlugins(numPlugins int,
	pluginService driver_infrastructure.PluginService,
	props map[string]string) driver_infrastructure.PluginManager {
	benchmarkPluginFactory := BenchmarkPluginFactory{}

	property_util.PLUGINS.Set(props, "")
	connectionProviderManager := driver_infrastructure.ConnectionProviderManager{DefaultProvider: &MockConnectionProvider{}}
	pluginManager := plugin_helpers.NewPluginManagerImpl(MockTargetDriver{}, props, connectionProviderManager)
	pluginChainBuilder := container.ConnectionPluginChainBuilder{}
	plugins, _ := pluginChainBuilder.GetPlugins(pluginService, pluginManager, props)

	for i := 0; i < numPlugins; i++ {
		plugin, _ := benchmarkPluginFactory.GetInstance(pluginService, props)
		plugins = append(plugins, plugin)
	}
	err := pluginManager.Init(pluginService, plugins)
	if err != nil {
		return nil
	}
	return pluginManager
}

func initPluginManagerWithNoPlugins(
	pluginService driver_infrastructure.PluginService,
	props map[string]string) driver_infrastructure.PluginManager {
	property_util.PLUGINS.Set(props, "")
	connectionProviderManager := driver_infrastructure.ConnectionProviderManager{DefaultProvider: &MockConnectionProvider{}}
	pluginManager := plugin_helpers.NewPluginManagerImpl(MockTargetDriver{}, props, connectionProviderManager)
	pluginChainBuilder := container.ConnectionPluginChainBuilder{}
	plugins, _ := pluginChainBuilder.GetPlugins(pluginService, pluginManager, props)

	err := pluginManager.Init(pluginService, plugins)
	if err != nil {
		return nil
	}
	return pluginManager
}

func BenchmarkConnectWithPlugins(b *testing.B) {
	props := make(map[string]string)
	pluginManager := initPluginManagerWithPlugins(BENCHMARK_DEFAULT_NUM_PLUGINS, &MockPluginService{}, props)
	host, _ := host_info_util.NewHostInfoBuilder().SetHost("host").SetPort(1234).Build()

	for i := 0; i < b.N; i++ {
		//nolint:errcheck
		pluginManager.Connect(
			host,
			props,
			true,
		)
	}
}

func BenchmarkConnectWithNoPlugins(b *testing.B) {
	props := make(map[string]string)
	pluginManager := initPluginManagerWithNoPlugins(&MockPluginService{}, props)
	host, _ := host_info_util.NewHostInfoBuilder().SetHost("host").SetPort(1234).Build()

	for i := 0; i < b.N; i++ {
		//nolint:errcheck
		pluginManager.Connect(
			host,
			props,
			true,
		)
	}
}

func BenchmarkExecuteWithPlugins(b *testing.B) {
	props := make(map[string]string)
	pluginManager := initPluginManagerWithPlugins(BENCHMARK_DEFAULT_NUM_PLUGINS, &MockPluginService{}, props)
	var calls []string
	execFunc := func() (any, any, bool, error) {
		calls = append(calls, "targetCall")
		return "resultTestValue", nil, true, nil
	}

	for i := 0; i < b.N; i++ {
		//nolint:errcheck
		pluginManager.Execute(
			"callA",
			execFunc,
			10,
			"arg2, 3.33",
		)
	}
}

func BenchmarkExecuteWithNoPlugins(b *testing.B) {
	props := make(map[string]string)
	pluginManager := initPluginManagerWithNoPlugins(&MockPluginService{}, props)
	var calls []string
	execFunc := func() (any, any, bool, error) {
		calls = append(calls, "targetCall")
		return "resultTestValue", nil, true, nil
	}

	for i := 0; i < b.N; i++ {
		//nolint:errcheck
		pluginManager.Execute(
			"callA",
			execFunc,
			10,
			"arg2, 3.33",
		)
	}
}

func BenchmarkInitHostProviderWithPlugins(b *testing.B) {
	props := make(map[string]string)
	pluginManager := initPluginManagerWithPlugins(BENCHMARK_DEFAULT_NUM_PLUGINS, &MockPluginService{}, props)

	for i := 0; i < b.N; i++ {
		//nolint:errcheck
		pluginManager.InitHostProvider(
			mysqlTestDsn,
			props,
			&MockRdsHostListProviderService{},
		)
	}
}

func BenchmarkInitHostProviderWithNoPlugins(b *testing.B) {
	props := make(map[string]string)
	pluginManager := initPluginManagerWithNoPlugins(&MockPluginService{}, props)

	for i := 0; i < b.N; i++ {
		//nolint:errcheck
		pluginManager.InitHostProvider(
			mysqlTestDsn,
			props,
			&MockRdsHostListProviderService{},
		)
	}
}

func BenchmarkNotifyConnectionChangedWithPlugins(b *testing.B) {
	props := make(map[string]string)
	pluginManager := initPluginManagerWithPlugins(BENCHMARK_DEFAULT_NUM_PLUGINS, &MockPluginService{}, props)

	hostChanged := map[driver_infrastructure.HostChangeOptions]bool{
		driver_infrastructure.HOST_CHANGED: true,
	}

	for i := 0; i < b.N; i++ {
		//nolint:errcheck
		pluginManager.NotifyConnectionChanged(
			hostChanged,
			nil,
		)
	}
}

func BenchmarkNotifyConnectionChangedWithNoPlugins(b *testing.B) {
	props := make(map[string]string)
	pluginManager := initPluginManagerWithNoPlugins(&MockPluginService{}, props)

	hostChanged := map[driver_infrastructure.HostChangeOptions]bool{
		driver_infrastructure.HOST_CHANGED: true,
	}

	for i := 0; i < b.N; i++ {
		//nolint:errcheck
		pluginManager.NotifyConnectionChanged(
			hostChanged,
			nil,
		)
	}
}

func BenchmarkReleaseResourcesWithPlugins(b *testing.B) {
	props := make(map[string]string)
	pluginManager := initPluginManagerWithPlugins(BENCHMARK_DEFAULT_NUM_PLUGINS, &MockPluginService{}, props)

	for i := 0; i < b.N; i++ {
		pluginManager.ReleaseResources()
	}
}

func BenchmarkReleaseResourcesWithNoPlugins(b *testing.B) {
	props := make(map[string]string)
	pluginManager := initPluginManagerWithNoPlugins(&MockPluginService{}, props)

	for i := 0; i < b.N; i++ {
		pluginManager.ReleaseResources()
	}
}
