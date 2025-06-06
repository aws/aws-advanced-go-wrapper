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
	"awssql/plugin_helpers"
	"awssql/property_util"
	"awssql/test_framework/container/test_utils"
	"awssql/utils"
	"awssql/utils/telemetry"
	"context"
	"database/sql/driver"
	"testing"
)

func getDefaultProps() map[string]string {
	props := make(map[string]string)
	property_util.USER.Set(props, "test")
	property_util.PASSWORD.Set(props, "mypassword")
	property_util.HOST.Set(props, "host")
	property_util.PORT.Set(props, "1234")
	property_util.DATABASE.Set(props, "mydb")
	property_util.PLUGINS.Set(props, "")
	property_util.DRIVER_PROTOCOL.Set(props, utils.MYSQL_DRIVER_PROTOCOL)
	property_util.ENABLE_TELEMETRY.Set(props, "true")
	property_util.TELEMETRY_TRACES_BACKEND.Set(props, "otlp")
	property_util.TELEMETRY_METRICS_BACKEND.Set(props, "otlp")
	return props
}

// TODO add telemetry.
func getPropsExecute() map[string]string {
	props := getDefaultProps()
	property_util.PLUGINS.Set(props, "executionTime")
	return props
}

func initResources(props map[string]string) (
	pluginManager driver_infrastructure.PluginManager,
	pluginService driver_infrastructure.PluginService,
	pluginManagerProvider driver_infrastructure.PluginManagerProvider,
	pluginServiceProvider driver_infrastructure.PluginServiceProvider,
	dsn string,
) {
	connectionProviderManager := driver_infrastructure.ConnectionProviderManager{DefaultProvider: &MockConnectionProvider{}}
	telemteryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	pluginManager = plugin_helpers.NewPluginManagerImpl(MockTargetDriver{}, props, connectionProviderManager, telemteryFactory)
	mockPluginService := &MockPluginService{}
	mockPluginService.PluginManager = pluginManager
	pluginService = driver_infrastructure.PluginService(mockPluginService)
	mockConn := &MockConn{}

	//nolint:errcheck
	pluginService.SetCurrentConnection(mockConn, nil, nil)

	pluginManagerProvider = func(
		targetDriver driver.Driver,
		props map[string]string,
		connProviderManager driver_infrastructure.ConnectionProviderManager,
		telemetryFactory telemetry.TelemetryFactory) driver_infrastructure.PluginManager {
		return pluginManager
	}

	pluginServiceProvider = func(
		pluginManager driver_infrastructure.PluginManager,
		driverDialect driver_infrastructure.DriverDialect,
		props map[string]string,
		dsn string) (driver_infrastructure.PluginService, error) {
		return pluginService, nil
	}

	dsn = test_utils.ConstructDsn(test_utils.MYSQL, props)
	return
}

func BenchmarkInitAndReleaseBaseline(b *testing.B) {
	props := getDefaultProps()
	manager, service, managerProvider, serviceProvider, dsn := initResources(props)

	for i := 1; i < b.N; i++ {
		testWrapper := NewTestConnectionWrapper(props, manager, service, managerProvider, serviceProvider, dsn, driver_infrastructure.MYSQL)
		testWrapper.PluginManager.ReleaseResources()
	}
}

func BenchmarkInitAndReleaseExecutionTime(b *testing.B) {
	// slog.SetLogLoggerLevel(slog.LevelDebug)
	props := getPropsExecute()
	manager, service, managerProvider, serviceProvider, dsn := initResources(props)

	for i := 1; i < b.N; i++ {
		testWrapper := NewTestConnectionWrapper(props, manager, service, managerProvider, serviceProvider, dsn, driver_infrastructure.MYSQL)
		testWrapper.PluginManager.ReleaseResources()
	}
}

func BenchmarkExecuteStatementBaseline(b *testing.B) {
	props := getDefaultProps()
	manager, service, managerProvider, serviceProvider, dsn := initResources(props)

	for i := 1; i < b.N; i++ {
		testWrapper := NewTestConnectionWrapper(props, manager, service, managerProvider, serviceProvider, dsn, driver_infrastructure.MYSQL)
		//nolint:errcheck
		testWrapper.AwsWrapperConn.QueryContext(context.TODO(), "Select 1", nil)
		testWrapper.PluginManager.ReleaseResources()
	}
}

func BenchmarkExecuteStatementWithExecutionTimePlugin(b *testing.B) {
	props := getPropsExecute()
	manager, service, managerProvider, serviceProvider, dsn := initResources(props)

	for i := 1; i < b.N; i++ {
		testWrapper := NewTestConnectionWrapper(props, manager, service, managerProvider, serviceProvider, dsn, driver_infrastructure.MYSQL)
		//nolint:errcheck
		testWrapper.AwsWrapperConn.QueryContext(context.TODO(), "Select 1", nil)
		testWrapper.PluginManager.ReleaseResources()
	}
}
