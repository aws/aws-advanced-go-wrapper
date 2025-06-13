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
	"context"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
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

func getPropsExecute() map[string]string {
	props := getDefaultProps()
	property_util.PLUGINS.Set(props, "executionTime")
	return props
}

func initResources(props map[string]string) (
	pluginManager driver_infrastructure.PluginManager,
	pluginService driver_infrastructure.PluginService,
) {
	connectionProviderManager := driver_infrastructure.ConnectionProviderManager{DefaultProvider: &MockConnectionProvider{}}
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	pluginManager = plugin_helpers.NewPluginManagerImpl(MockTargetDriver{}, props, connectionProviderManager, telemetryFactory)
	mockPluginService := &MockPluginService{}
	mockPluginService.PluginManager = pluginManager
	pluginService = driver_infrastructure.PluginService(mockPluginService)
	mockConn := &MockConn{}

	//nolint:errcheck
	pluginService.SetCurrentConnection(mockConn, nil, nil)
	return
}

func BenchmarkInitAndReleaseBaseline(b *testing.B) {
	props := getDefaultProps()
	manager, service := initResources(props)

	for i := 1; i < b.N; i++ {
		testWrapper := NewTestConnectionWrapper(manager, service, driver_infrastructure.MYSQL)
		testWrapper.PluginManager.ReleaseResources()
	}
}

func BenchmarkInitAndReleaseExecutionTime(b *testing.B) {
	props := getPropsExecute()
	manager, service := initResources(props)

	for i := 1; i < b.N; i++ {
		testWrapper := NewTestConnectionWrapper(manager, service, driver_infrastructure.MYSQL)
		testWrapper.PluginManager.ReleaseResources()
	}
}

func BenchmarkExecuteStatementBaseline(b *testing.B) {
	props := getDefaultProps()
	manager, service := initResources(props)

	for i := 1; i < b.N; i++ {
		testWrapper := NewTestConnectionWrapper(manager, service, driver_infrastructure.MYSQL)
		//nolint:errcheck
		testWrapper.AwsWrapperConn.QueryContext(context.TODO(), "Select 1", nil)
		testWrapper.PluginManager.ReleaseResources()
	}
}

func BenchmarkExecuteStatementWithExecutionTimePlugin(b *testing.B) {
	props := getPropsExecute()
	manager, service := initResources(props)

	for i := 1; i < b.N; i++ {
		testWrapper := NewTestConnectionWrapper(manager, service, driver_infrastructure.MYSQL)
		//nolint:errcheck
		testWrapper.AwsWrapperConn.QueryContext(context.TODO(), "Select 1", nil)
		testWrapper.PluginManager.ReleaseResources()
	}
}
