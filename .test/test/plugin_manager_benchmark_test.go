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
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
)

var BENCHMARK_DEFAULT_NUM_PLUGINS int = 10

func initPluginManagerWithPlugins(numPlugins int,
	props map[string]string) driver_infrastructure.PluginManager {
	property_util.PLUGINS.Set(props, "")
	property_util.ENABLE_TELEMETRY.Set(props, "true")
	property_util.TELEMETRY_TRACES_BACKEND.Set(props, "none")
	property_util.TELEMETRY_METRICS_BACKEND.Set(props, "none")
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)

	benchmarkPluginFactory := BenchmarkPluginFactory{}
	connectionProviderManager := driver_infrastructure.ConnectionProviderManager{DefaultProvider: &MockConnectionProvider{}}
	pluginManager := plugin_helpers.NewPluginManagerImpl(MockTargetDriver{}, props, connectionProviderManager, telemetryFactory)
	pluginService := &MockPluginService{}

	pluginChainBuilder := driver.ConnectionPluginChainBuilder{}
	plugins, _ := pluginChainBuilder.GetPlugins(pluginService, pluginManager, props, defaultPluginFactoryByCode)
	for i := 0; i < numPlugins; i++ {
		plugin, _ := benchmarkPluginFactory.GetInstance(pluginService, props)
		plugins = append(plugins, plugin)
	}
	err := pluginManager.Init(pluginService, plugins)
	if err != nil {
		return nil
	}
	pluginService.PluginManager = pluginManager
	return pluginManager
}

func initPluginManagerWithNoPlugins(
	props map[string]string) driver_infrastructure.PluginManager {
	property_util.PLUGINS.Set(props, "")
	property_util.ENABLE_TELEMETRY.Set(props, "true")
	property_util.TELEMETRY_TRACES_BACKEND.Set(props, "none")
	property_util.TELEMETRY_METRICS_BACKEND.Set(props, "none")
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	connectionProviderManager := driver_infrastructure.ConnectionProviderManager{DefaultProvider: &MockConnectionProvider{}}
	pluginManager := plugin_helpers.NewPluginManagerImpl(MockTargetDriver{}, props, connectionProviderManager, telemetryFactory)
	pluginService := &MockPluginService{}

	pluginChainBuilder := driver.ConnectionPluginChainBuilder{}
	plugins, _ := pluginChainBuilder.GetPlugins(pluginService, pluginManager, props, defaultPluginFactoryByCode)

	err := pluginManager.Init(pluginService, plugins)
	if err != nil {
		return nil
	}
	pluginService.PluginManager = pluginManager
	return pluginManager
}

func BenchmarkConnectWith10Plugins(b *testing.B) {
	props := make(map[string]string)
	pluginManager := initPluginManagerWithPlugins(BENCHMARK_DEFAULT_NUM_PLUGINS, props)
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
	pluginManager := initPluginManagerWithNoPlugins(props)
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

func BenchmarkExecuteWith10Plugins(b *testing.B) {
	props := make(map[string]string)
	pluginManager := initPluginManagerWithPlugins(BENCHMARK_DEFAULT_NUM_PLUGINS, props)
	var calls []string
	execFunc := func() (any, any, bool, error) {
		calls = append(calls, "targetCall")
		return "resultTestValue", nil, true, nil
	}

	for i := 0; i < b.N; i++ {
		//nolint:errcheck
		pluginManager.Execute(
			nil,
			"callA",
			execFunc,
			10,
			"arg2, 3.33",
		)
	}
}

func BenchmarkExecuteWithNoPlugins(b *testing.B) {
	props := make(map[string]string)
	pluginManager := initPluginManagerWithNoPlugins(props)
	var calls []string
	execFunc := func() (any, any, bool, error) {
		calls = append(calls, "targetCall")
		return "resultTestValue", nil, true, nil
	}

	for i := 0; i < b.N; i++ {
		//nolint:errcheck
		pluginManager.Execute(
			nil,
			"callA",
			execFunc,
			10,
			"arg2, 3.33",
		)
	}
}

func BenchmarkInitHostProviderWith10Plugins(b *testing.B) {
	props := make(map[string]string)
	pluginManager := initPluginManagerWithPlugins(BENCHMARK_DEFAULT_NUM_PLUGINS, props)

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
	pluginManager := initPluginManagerWithNoPlugins(props)

	for i := 0; i < b.N; i++ {
		//nolint:errcheck
		pluginManager.InitHostProvider(
			mysqlTestDsn,
			props,
			&MockRdsHostListProviderService{},
		)
	}
}

func BenchmarkNotifyConnectionChangedWith10Plugins(b *testing.B) {
	props := make(map[string]string)
	pluginManager := initPluginManagerWithPlugins(BENCHMARK_DEFAULT_NUM_PLUGINS, props)

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
	pluginManager := initPluginManagerWithNoPlugins(props)

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

func BenchmarkReleaseResourcesWith10Plugins(b *testing.B) {
	props := make(map[string]string)
	pluginManager := initPluginManagerWithPlugins(BENCHMARK_DEFAULT_NUM_PLUGINS, props)

	for i := 0; i < b.N; i++ {
		pluginManager.ReleaseResources()
	}
}

func BenchmarkReleaseResourcesWithNoPlugins(b *testing.B) {
	props := make(map[string]string)
	pluginManager := initPluginManagerWithNoPlugins(props)

	for i := 0; i < b.N; i++ {
		pluginManager.ReleaseResources()
	}
}
