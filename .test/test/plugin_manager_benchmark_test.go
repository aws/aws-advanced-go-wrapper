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
	"fmt"
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
)

var BENCHMARK_DEFAULT_NUM_PLUGINS = 10
var PLUGIN_COUNTS = []int{0, 1, 2, 5, 10}

func initPluginManagerWithPlugins(numPlugins int,
	props *utils.RWMap[string, string]) driver_infrastructure.PluginManager {
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
		plugins = append([]driver_infrastructure.ConnectionPlugin{plugin}, plugins...)
	}
	err := pluginManager.Init(pluginService, plugins)
	if err != nil {
		return nil
	}
	pluginService.PluginManager = pluginManager
	return pluginManager
}

func BenchmarkConnectWithPlugins(b *testing.B) {
	props := utils.NewRWMap[string, string]()
	host, _ := host_info_util.NewHostInfoBuilder().SetHost("host").SetPort(1234).Build()

	for _, count := range PLUGIN_COUNTS {
		count := count // capture range variable
		b.Run(fmt.Sprintf("%d_Plugins", count), func(b *testing.B) {
			pluginManager := initPluginManagerWithPlugins(count, props)

			b.ResetTimer() // reset timer to ignore setup time
			for i := 0; i < b.N; i++ {
				//nolint:errcheck
				_, _ = pluginManager.Connect(
					host,
					props,
					true,
					nil,
				)
			}
			pluginManager.ReleaseResources()
		})
	}
}

func BenchmarkExecute(b *testing.B) {
	props := utils.NewRWMap[string, string]()

	for _, count := range PLUGIN_COUNTS {
		count := count // capture range variable
		b.Run(fmt.Sprintf("%d_Plugins", count), func(b *testing.B) {
			pluginManager := initPluginManagerWithPlugins(count, props)

			b.ResetTimer() // reset timer to ignore setup time
			for i := 0; i < b.N; i++ {
				//nolint:errcheck
				_, _, _, _ = pluginManager.Execute(
					nil,
					"callA",
					execFunc,
					10,
					"arg2, 3.33",
				)
			}
			pluginManager.ReleaseResources()
		})
	}
}

func BenchmarkInitHostProvider(b *testing.B) {
	props, _ := property_util.ParseDsn(mysqlTestDsn)

	for _, count := range PLUGIN_COUNTS {
		count := count // capture range variable
		b.Run(fmt.Sprintf("%d_Plugins", count), func(b *testing.B) {
			pluginManager := initPluginManagerWithPlugins(count, props)

			b.ResetTimer() // reset timer to ignore setup time
			for i := 0; i < b.N; i++ {
				//nolint:errcheck
				_ = pluginManager.InitHostProvider(
					props,
					&MockRdsHostListProviderService{},
				)
			}
			pluginManager.ReleaseResources()
		})
	}
}

func BenchmarkNotifyConnectionChanged(b *testing.B) {
	props := utils.NewRWMap[string, string]()
	hostChanged := map[driver_infrastructure.HostChangeOptions]bool{
		driver_infrastructure.HOST_CHANGED: true,
	}
	for _, count := range PLUGIN_COUNTS {
		count := count // capture range variable
		b.Run(fmt.Sprintf("%d_Plugins", count), func(b *testing.B) {
			pluginManager := initPluginManagerWithPlugins(count, props)

			b.ResetTimer() // reset timer to ignore setup time
			for i := 0; i < b.N; i++ {
				//nolint:errcheck
				pluginManager.NotifyConnectionChanged(
					hostChanged,
					nil,
				)
			}
			pluginManager.ReleaseResources()
		})
	}
}

func BenchmarkReleaseResources(b *testing.B) {
	props := utils.NewRWMap[string, string]()
	for _, count := range PLUGIN_COUNTS {
		count := count // capture range variable
		b.Run(fmt.Sprintf("%d_Plugins", count), func(b *testing.B) {
			pluginManager := initPluginManagerWithPlugins(count, props)

			b.ResetTimer() // reset timer to ignore setup time
			for i := 0; i < b.N; i++ {
				//nolint:errcheck
				pluginManager.ReleaseResources()
			}
			pluginManager.ReleaseResources()
		})
	}
}
