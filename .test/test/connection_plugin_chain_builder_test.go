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
	"reflect"
	"testing"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	awsDriver "github.com/aws/aws-advanced-go-wrapper/awssql/driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/efm"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/limitless"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	"github.com/aws/aws-advanced-go-wrapper/iam"
	_ "github.com/aws/aws-advanced-go-wrapper/iam"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSortPlugins(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginManager := mock_driver_infrastructure.NewMockPluginManager(ctrl)
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)

	mockPluginService.EXPECT().GetTelemetryFactory().Return(telemetry.NewNilTelemetryFactory()).AnyTimes()
	mockPluginManager.EXPECT().GetDefaultConnectionProvider().Return(nil).AnyTimes()
	mockPluginManager.EXPECT().GetEffectiveConnectionProvider().Return(nil).AnyTimes()
	mockPluginManager.EXPECT().GetConnectionProviderManager().Return(driver_infrastructure.ConnectionProviderManager{}).AnyTimes()

	builder := &awsDriver.ConnectionPluginChainBuilder{}
	props := map[string]string{property_util.PLUGINS.Name: "iam,efm,failover"}
	availablePlugins := map[string]driver_infrastructure.ConnectionPluginFactory{
		"failover":      plugins.NewFailoverPluginFactory(),
		"efm":           efm.NewHostMonitoringPluginFactory(),
		"limitless":     limitless.NewLimitlessPluginFactory(),
		"executionTime": plugins.NewExecutionTimePluginFactory(),
		"iam":           iam.NewIamAuthPluginFactory(),
	}
	plugins, err := builder.GetPlugins(mockPluginService, mockPluginManager, props, availablePlugins)
	require.Nil(t, err)

	assert.Equal(t, 4, len(plugins), "Expected 4 plugins.")
	assert.Equal(t, "*plugins.FailoverPlugin", reflect.TypeOf(plugins[0]).String())
	assert.Equal(t, "*efm.HostMonitorConnectionPlugin", reflect.TypeOf(plugins[1]).String())
	assert.Equal(t, "*iam.IamAuthPlugin", reflect.TypeOf(plugins[2]).String())
	assert.Equal(t, "*plugins.DefaultPlugin", reflect.TypeOf(plugins[3]).String())
}

func TestPreservePluginOrder(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginManager := mock_driver_infrastructure.NewMockPluginManager(ctrl)
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)

	mockPluginService.EXPECT().GetTelemetryFactory().Return(telemetry.NewNilTelemetryFactory()).AnyTimes()
	mockPluginManager.EXPECT().GetDefaultConnectionProvider().Return(nil).AnyTimes()
	mockPluginManager.EXPECT().GetEffectiveConnectionProvider().Return(nil).AnyTimes()
	mockPluginManager.EXPECT().GetConnectionProviderManager().Return(driver_infrastructure.ConnectionProviderManager{}).AnyTimes()

	builder := &awsDriver.ConnectionPluginChainBuilder{}
	props := map[string]string{property_util.PLUGINS.Name: "iam,efm,failover", property_util.AUTO_SORT_PLUGIN_ORDER.Name: "false"}
	availablePlugins := map[string]driver_infrastructure.ConnectionPluginFactory{
		"failover":      plugins.NewFailoverPluginFactory(),
		"efm":           efm.NewHostMonitoringPluginFactory(),
		"limitless":     limitless.NewLimitlessPluginFactory(),
		"executionTime": plugins.NewExecutionTimePluginFactory(),
		"iam":           iam.NewIamAuthPluginFactory(),
	}
	plugins, err := builder.GetPlugins(mockPluginService, mockPluginManager, props, availablePlugins)
	require.Nil(t, err)

	assert.Equal(t, 4, len(plugins), "Expected 4 plugins.")
	assert.Equal(t, "*iam.IamAuthPlugin", reflect.TypeOf(plugins[0]).String())
	assert.Equal(t, "*efm.HostMonitorConnectionPlugin", reflect.TypeOf(plugins[1]).String())
	assert.Equal(t, "*plugins.FailoverPlugin", reflect.TypeOf(plugins[2]).String())
	assert.Equal(t, "*plugins.DefaultPlugin", reflect.TypeOf(plugins[3]).String())
}

func TestSortAllPlugins(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginManager := mock_driver_infrastructure.NewMockPluginManager(ctrl)
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)

	mockPluginService.EXPECT().GetTelemetryFactory().Return(telemetry.NewNilTelemetryFactory()).AnyTimes()
	mockPluginManager.EXPECT().GetDefaultConnectionProvider().Return(nil).AnyTimes()
	mockPluginManager.EXPECT().GetEffectiveConnectionProvider().Return(nil).AnyTimes()
	mockPluginManager.EXPECT().GetConnectionProviderManager().Return(driver_infrastructure.ConnectionProviderManager{}).AnyTimes()

	builder := &awsDriver.ConnectionPluginChainBuilder{}
	props := map[string]string{property_util.PLUGINS.Name: "iam,executionTime,limitless,efm,failover"}
	availablePlugins := map[string]driver_infrastructure.ConnectionPluginFactory{
		"failover":      plugins.NewFailoverPluginFactory(),
		"efm":           efm.NewHostMonitoringPluginFactory(),
		"limitless":     limitless.NewLimitlessPluginFactory(),
		"executionTime": plugins.NewExecutionTimePluginFactory(),
		"iam":           iam.NewIamAuthPluginFactory(),
	}
	plugins, err := builder.GetPlugins(mockPluginService, mockPluginManager, props, availablePlugins)
	require.Nil(t, err)

	assert.Equal(t, 6, len(plugins), "Expected 6 plugins.")
	assert.Equal(t, "*plugins.ExecutionTimePlugin", reflect.TypeOf(plugins[0]).String())
	assert.Equal(t, "*limitless.LimitlessPlugin", reflect.TypeOf(plugins[1]).String())
	assert.Equal(t, "*plugins.FailoverPlugin", reflect.TypeOf(plugins[2]).String())
	assert.Equal(t, "*efm.HostMonitorConnectionPlugin", reflect.TypeOf(plugins[3]).String())
	assert.Equal(t, "*iam.IamAuthPlugin", reflect.TypeOf(plugins[4]).String())
	assert.Equal(t, "*plugins.DefaultPlugin", reflect.TypeOf(plugins[5]).String())
}

func TestNoPlugins(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginManager := mock_driver_infrastructure.NewMockPluginManager(ctrl)
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)

	mockPluginService.EXPECT().GetTelemetryFactory().Return(telemetry.NewNilTelemetryFactory()).AnyTimes()
	mockPluginManager.EXPECT().GetDefaultConnectionProvider().Return(nil).AnyTimes()
	mockPluginManager.EXPECT().GetEffectiveConnectionProvider().Return(nil).AnyTimes()
	mockPluginManager.EXPECT().GetConnectionProviderManager().Return(driver_infrastructure.ConnectionProviderManager{}).AnyTimes()

	builder := &awsDriver.ConnectionPluginChainBuilder{}
	props := map[string]string{property_util.PLUGINS.Name: "none"}
	availablePlugins := map[string]driver_infrastructure.ConnectionPluginFactory{
		"failover":      plugins.NewFailoverPluginFactory(),
		"efm":           efm.NewHostMonitoringPluginFactory(),
		"limitless":     limitless.NewLimitlessPluginFactory(),
		"executionTime": plugins.NewExecutionTimePluginFactory(),
		"iam":           iam.NewIamAuthPluginFactory(),
	}
	plugins, err := builder.GetPlugins(mockPluginService, mockPluginManager, props, availablePlugins)
	require.Nil(t, err)

	assert.Equal(t, 1, len(plugins), "Expected 1 plugin.")
	assert.Equal(t, "*plugins.DefaultPlugin", reflect.TypeOf(plugins[0]).String())
}
