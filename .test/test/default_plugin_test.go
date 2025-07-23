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
	"errors"
	"testing"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	mock_telemetry "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/util/telemetry"
	mock_database_sql_driver "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/database_sql_driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestDefaultPlugin_InitHostProvider(t *testing.T) {
	dp := &plugins.DefaultPlugin{}
	err := dp.InitHostProvider(map[string]string{}, nil, nil)
	assert.NoError(t, err)
}

func TestDefaultPlugin_GetSubscribedMethods(t *testing.T) {
	dp := &plugins.DefaultPlugin{}
	methods := dp.GetSubscribedMethods()
	assert.Equal(t, []string{plugin_helpers.ALL_METHODS}, methods)
}

func TestDefaultPlugin_Execute_OpenTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockConn := mock_database_sql_driver.NewMockConn(ctrl)

	mockPluginService.EXPECT().GetCurrentConnection().Return(mockConn).Times(1)
	mockPluginService.EXPECT().SetInTransaction(true).Times(1)

	dp := &plugins.DefaultPlugin{
		PluginService: mockPluginService,
	}

	methodName := "Conn.Begin" // matches open transaction
	executeFunc := func() (any, any, bool, error) {
		return "value", nil, true, nil
	}

	rv1, rv2, ok, err := dp.Execute(mockConn, methodName, executeFunc)

	assert.Equal(t, "value", rv1)
	assert.Nil(t, rv2)
	assert.True(t, ok)
	assert.NoError(t, err)
}

func TestDefaultPlugin_Execute_CloseTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockConn := mock_database_sql_driver.NewMockConn(ctrl)

	mockPluginService.EXPECT().GetCurrentConnection().Return(mockConn).Times(1)
	mockPluginService.EXPECT().SetInTransaction(false).Times(1)

	dp := &plugins.DefaultPlugin{
		PluginService: mockPluginService,
	}

	methodName := "Tx.Commit"
	executeFunc := func() (any, any, bool, error) {
		return "done", nil, true, nil
	}

	rv1, _, ok, err := dp.Execute(mockConn, methodName, executeFunc)

	assert.Equal(t, "done", rv1)
	assert.True(t, ok)
	assert.NoError(t, err)
}

func TestDefaultPlugin_Execute_WithError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)

	dp := &plugins.DefaultPlugin{
		PluginService: mockPluginService,
	}

	executeFunc := func() (any, any, bool, error) {
		return nil, nil, false, errors.New("execution error")
	}

	_, _, _, err := dp.Execute(nil, "select", executeFunc)
	assert.Error(t, err)
}

func TestDefaultPlugin_Execute_WithOldConnection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockConnCurrent := mock_database_sql_driver.NewMockConn(ctrl)
	mockConnOld := mock_database_sql_driver.NewMockConn(ctrl)

	mockPluginService.EXPECT().GetCurrentConnection().Return(mockConnCurrent).Times(1)

	dp := &plugins.DefaultPlugin{
		PluginService: mockPluginService,
	}

	executeFunc := func() (any, any, bool, error) {
		return "old-ok", nil, true, nil
	}

	rv1, rv2, ok, err := dp.Execute(mockConnOld, "begin", executeFunc)

	assert.Equal(t, "old-ok", rv1)
	assert.Nil(t, rv2)
	assert.True(t, ok)
	assert.NoError(t, err)
}

func TestDefaultPlugin_Connect_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Mocks
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockTelemetry := mock_telemetry.NewMockTelemetryFactory(ctrl)
	mockTelemetryCtx := mock_telemetry.NewMockTelemetryContext(ctrl)
	mockConnProvider := mock_driver_infrastructure.NewMockConnectionProvider(ctrl)
	mockConn := mock_database_sql_driver.NewMockConn(ctrl)

	// Real connection provider manager using mocked ConnectionProvider
	connProviderManager := &driver_infrastructure.ConnectionProviderManager{
		DefaultProvider: mockConnProvider,
	}

	// HostInfo with valid alias map
	hostInfo := &host_info_util.HostInfo{
		Host:       "localhost",
		Port:       5432,
		AllAliases: map[string]bool{"localhost": true},
	}
	props := map[string]string{}

	// Create dummy contexts
	ctxBefore := context.Background()
	ctxDuring := context.Background()

	// Telemetry expectations
	mockPluginService.EXPECT().GetTelemetryContext().Return(ctxBefore).Times(1)
	mockPluginService.EXPECT().GetTelemetryFactory().Return(mockTelemetry).Times(1)
	mockTelemetry.EXPECT().
		OpenTelemetryContext(gomock.Any(), telemetry.NESTED, ctxBefore).
		Return(mockTelemetryCtx, ctxDuring).Times(1)

	mockPluginService.EXPECT().SetTelemetryContext(ctxDuring).Times(1)
	mockPluginService.EXPECT().SetTelemetryContext(ctxBefore).Times(1)
	mockTelemetryCtx.EXPECT().CloseContext().Times(1)

	// Connection expectation
	mockConnProvider.EXPECT().
		Connect(hostInfo, props, mockPluginService).
		Return(mockConn, nil).Times(1)

	// Post-connection behaviors
	mockPluginService.EXPECT().SetAvailability(hostInfo.AllAliases, host_info_util.AVAILABLE).Times(1)
	mockPluginService.EXPECT().UpdateDialect(mockConn).Times(1)

	// Test subject
	dp := &plugins.DefaultPlugin{
		PluginService:       mockPluginService,
		ConnProviderManager: *connProviderManager,
	}

	conn, err := dp.Connect(hostInfo, props, true, nil)
	assert.NoError(t, err)
	assert.Equal(t, mockConn, conn)
}

func TestDefaultPlugin_ForceConnect_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockTelemetry := mock_telemetry.NewMockTelemetryFactory(ctrl)
	mockTelemetryCtx := mock_telemetry.NewMockTelemetryContext(ctrl)
	mockConnProvider := mock_driver_infrastructure.NewMockConnectionProvider(ctrl)
	mockConn := mock_database_sql_driver.NewMockConn(ctrl)

	// Use a valid HostInfo with map[string]bool for AllAliases
	hostInfo := &host_info_util.HostInfo{
		Host:       "primary",
		Port:       5432,
		AllAliases: map[string]bool{"primary": true},
	}
	props := map[string]string{}

	// Proper context objects
	ctxBefore := context.Background()
	ctxDuring := context.Background()

	// Telemetry expectations
	mockPluginService.EXPECT().GetTelemetryContext().Return(ctxBefore).Times(1)
	mockPluginService.EXPECT().GetTelemetryFactory().Return(mockTelemetry).Times(1)
	mockTelemetry.EXPECT().
		OpenTelemetryContext(gomock.Any(), telemetry.NESTED, ctxBefore).
		Return(mockTelemetryCtx, ctxDuring).Times(1)

	mockPluginService.EXPECT().SetTelemetryContext(ctxDuring).Times(1)
	mockPluginService.EXPECT().SetTelemetryContext(ctxBefore).Times(1)
	mockTelemetryCtx.EXPECT().CloseContext().Times(1)

	// Connection expectation
	mockConnProvider.EXPECT().
		Connect(hostInfo, props, mockPluginService).
		Return(mockConn, nil).Times(1)

	mockPluginService.EXPECT().SetAvailability(hostInfo.AllAliases, host_info_util.AVAILABLE).Times(1)
	mockPluginService.EXPECT().UpdateDialect(mockConn).Times(1)

	dp := &plugins.DefaultPlugin{
		PluginService:       mockPluginService,
		DefaultConnProvider: mockConnProvider,
	}

	conn, err := dp.ForceConnect(hostInfo, props, true, nil)
	assert.NoError(t, err)
	assert.Equal(t, mockConn, conn)
}

func TestDefaultPlugin_AcceptsStrategy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Mock ConnectionProvider to respond to AcceptsStrategy
	mockConnProvider := mock_driver_infrastructure.NewMockConnectionProvider(ctrl)
	mockConnProvider.EXPECT().
		AcceptsStrategy("rr").
		Return(true).Times(1)

	// Use real struct with mocked DefaultProvider
	connProviderManager := &driver_infrastructure.ConnectionProviderManager{
		DefaultProvider: mockConnProvider,
	}

	dp := &plugins.DefaultPlugin{
		ConnProviderManager: *connProviderManager,
	}

	assert.True(t, dp.AcceptsStrategy("rr"))
}

func TestDefaultPlugin_GetHostInfoByStrategy_NoHosts(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)

	dp := &plugins.DefaultPlugin{
		PluginService: mockPluginService,
	}

	host, err := dp.GetHostInfoByStrategy(host_info_util.READER, "xyz", []*host_info_util.HostInfo{})
	assert.Nil(t, host)
	assert.Error(t, err)
}

func TestDefaultPlugin_NotifyConnectionChanged(t *testing.T) {
	dp := &plugins.DefaultPlugin{}
	result := dp.NotifyConnectionChanged(nil)
	assert.Equal(t, driver_infrastructure.NO_OPINION, result)
}

func TestDefaultPlugin_NotifyHostListChanged(t *testing.T) {
	dp := &plugins.DefaultPlugin{}
	dp.NotifyHostListChanged(nil) // should be a no-op, just assert no panic
}
