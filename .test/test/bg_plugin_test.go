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
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	mock_telemetry "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/util/telemetry"
	mock_database_sql_driver "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/database_sql_driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/bg"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/services"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestStorageService creates a real ExpiringStorage with BG status type registered.
func newTestStorageService() *services.ExpiringStorage {
	storage := services.NewExpiringStorage(5*time.Minute, nil)
	driver_infrastructure.RegisterDefaultStorageTypes(storage)
	return storage
}

// bgStatusCacheKey returns the cache key used by BlueGreenPlugin for a given bgId.
func bgStatusCacheKey(bgId string) string {
	return bgId + "::BlueGreenStatus"
}

// setupBgServicesContainer creates a MockServicesContainer wired to the given MockPluginService and storage.
func setupBgServicesContainer(
	ctrl *gomock.Controller,
	mockPluginService *mock_driver_infrastructure.MockPluginService,
	storage driver_infrastructure.StorageService,
) *mock_driver_infrastructure.MockServicesContainer {
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()
	mockContainer.EXPECT().GetStorageService().Return(storage).AnyTimes()
	return mockContainer
}

func TestBlueGreenPluginFactory_GetInstance(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	factory := bg.NewBlueGreenPluginFactory()
	require.NotNil(t, factory)
	assert.IsType(t, &bg.BlueGreenPluginFactory{}, factory)

	storage := newTestStorageService()
	defer storage.Stop()
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := setupBgServicesContainer(ctrl, mockPluginService, storage)

	plugin, err := factory.GetInstance(mockContainer, MakeMapFromKeysAndVals(
		property_util.BGD_ID.Name, "",
	))
	assert.Nil(t, plugin)
	require.NotNil(t, err)
	assert.Equal(t, error_util.GetMessage("BlueGreenDeployment.bgIdRequired"), err.Error())

	plugin, err = factory.GetInstance(mockContainer, MakeMapFromKeysAndVals(
		property_util.BGD_ID.Name, "test-bg-id",
	))
	assert.NoError(t, err)
	assert.NotNil(t, plugin)
	assert.IsType(t, &bg.BlueGreenPlugin{}, plugin)
}

func TestBlueGreenPlugin_GetSubscribedMethods(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storage := newTestStorageService()
	defer storage.Stop()
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := setupBgServicesContainer(ctrl, mockPluginService, storage)
	props := MakeMapFromKeysAndVals(
		property_util.BGD_ID.Name, "test-bg-id",
	)

	plugin, err := bg.NewBlueGreenPlugin(mockContainer, props)
	assert.NoError(t, err)

	methods := plugin.GetSubscribedMethods()

	assert.NotEmpty(t, methods)
	assert.Contains(t, methods, "Conn.Connect")
	assert.Contains(t, methods, "Conn.QueryContext")
	assert.Contains(t, methods, "Stmt.ExecContext")
}

func TestBlueGreenPlugin_Connect(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	connectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		return mockConn, nil
	}
	hostInfo := &host_info_util.HostInfo{Host: "test-host"}
	roleByHost := utils.NewRWMap[string, driver_infrastructure.BlueGreenRole]()
	correspondingHosts := utils.NewRWMap[string, utils.Pair[*host_info_util.HostInfo, *host_info_util.HostInfo]]()
	connectRouting := bg.NewSubstituteConnectRouting(hostInfo.GetHostAndPort(), driver_infrastructure.SOURCE, hostInfo, nil, nil)
	bgStatus := driver_infrastructure.NewBgStatus("test-bg-id", driver_infrastructure.CREATED, []driver_infrastructure.ConnectRouting{connectRouting},
		nil, roleByHost, correspondingHosts)
	props := MakeMapFromKeysAndVals(
		property_util.BGD_ID.Name, "test-bg-id",
	)
	defer (&bg.BlueGreenPluginFactory{}).ClearCaches()

	t.Run("NoBlueGreenStatus", func(t *testing.T) {
		storage := newTestStorageService()
		defer storage.Stop()
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockContainer := setupBgServicesContainer(ctrl, mockPluginService, storage)
		// No BG status set in storage — getBgStatus returns nil
		mockPluginService.EXPECT().GetDialect().Return(&driver_infrastructure.MySQLDatabaseDialect{}).AnyTimes()

		plugin, err := bg.NewBlueGreenPlugin(mockContainer, props)
		assert.NoError(t, err)

		conn, err := plugin.Connect(hostInfo, props, true, connectFunc)

		assert.NoError(t, err)
		assert.Equal(t, mockConn, conn)
	})

	t.Run("InitialConnectionWithIAM", func(t *testing.T) {
		storage := newTestStorageService()
		defer storage.Stop()
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockContainer := setupBgServicesContainer(ctrl, mockPluginService, storage)

		plugin, err := bg.NewBlueGreenPlugin(mockContainer, props)
		assert.NoError(t, err)

		// Set BG status in storage
		driver_infrastructure.BlueGreenStatusStorageType.Set(storage, bgStatusCacheKey("test-bg-id"), &bgStatus)
		mockPluginService.EXPECT().IsPluginInUse(driver_infrastructure.IAM_PLUGIN_CODE).Return(true)
		mockPluginService.EXPECT().GetDialect().Return(&driver_infrastructure.MySQLDatabaseDialect{}).AnyTimes()

		conn, err := plugin.Connect(hostInfo, props, true, connectFunc)

		assert.NoError(t, err)
		assert.Equal(t, mockConn, conn)
	})

	t.Run("NoMatchingHostRole", func(t *testing.T) {
		storage := newTestStorageService()
		defer storage.Stop()
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockContainer := setupBgServicesContainer(ctrl, mockPluginService, storage)

		plugin, err := bg.NewBlueGreenPlugin(mockContainer, props)
		assert.NoError(t, err)

		driver_infrastructure.BlueGreenStatusStorageType.Set(storage, bgStatusCacheKey("test-bg-id"), &bgStatus)
		mockPluginService.EXPECT().IsPluginInUse(driver_infrastructure.IAM_PLUGIN_CODE).Return(false)
		mockPluginService.EXPECT().GetDialect().Return(&driver_infrastructure.MySQLDatabaseDialect{}).AnyTimes()

		conn, err := plugin.Connect(hostInfo, props, true, connectFunc)

		assert.NoError(t, err)
		assert.Equal(t, mockConn, conn)
	})

	t.Run("NoMatchingRoutes", func(t *testing.T) {
		storage := newTestStorageService()
		defer storage.Stop()
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockContainer := setupBgServicesContainer(ctrl, mockPluginService, storage)
		roleByHost.Put(hostInfo.GetHost(), driver_infrastructure.TARGET)

		plugin, err := bg.NewBlueGreenPlugin(mockContainer, props)
		assert.NoError(t, err)

		driver_infrastructure.BlueGreenStatusStorageType.Set(storage, bgStatusCacheKey("test-bg-id"), &bgStatus)
		mockPluginService.EXPECT().IsPluginInUse(driver_infrastructure.IAM_PLUGIN_CODE).Return(false)
		mockPluginService.EXPECT().GetDialect().Return(&driver_infrastructure.MySQLDatabaseDialect{}).AnyTimes()

		conn, err := plugin.Connect(hostInfo, props, true, connectFunc)

		assert.NoError(t, err)
		assert.Equal(t, mockConn, conn)
	})

	t.Run("RoutingConnects", func(t *testing.T) {
		storage := newTestStorageService()
		defer storage.Stop()
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockContainer := setupBgServicesContainer(ctrl, mockPluginService, storage)
		roleByHost.Put(hostInfo.GetHost(), driver_infrastructure.SOURCE)

		plugin, err := bg.NewBlueGreenPlugin(mockContainer, props)
		assert.NoError(t, err)

		driver_infrastructure.BlueGreenStatusStorageType.Set(storage, bgStatusCacheKey("test-bg-id"), &bgStatus)
		mockPluginService.EXPECT().GetDialect().Return(&driver_infrastructure.MySQLDatabaseDialect{}).AnyTimes()
		mockPluginService.EXPECT().IsPluginInUse(driver_infrastructure.IAM_PLUGIN_CODE).Return(false).AnyTimes()
		mockPluginService.EXPECT().Connect(hostInfo, props, gomock.Any()).Return(mockConn, nil)

		conn, err := plugin.Connect(hostInfo, props, true, connectFunc)

		assert.NoError(t, err)
		assert.NotNil(t, conn)
	})
}

func TestBlueGreenPlugin_Execute(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	props := MakeMapFromKeysAndVals(
		property_util.BGD_ID.Name, "test-bg-id",
	)
	executeFunc := func() (any, any, bool, error) {
		return "result", nil, true, nil
	}
	roleByHost := utils.NewRWMap[string, driver_infrastructure.BlueGreenRole]()
	correspondingHosts := utils.NewRWMap[string, utils.Pair[*host_info_util.HostInfo, *host_info_util.HostInfo]]()
	hostInfo := &host_info_util.HostInfo{Host: "test-host"}
	bgStatus := driver_infrastructure.NewBgStatus("test-bg-id", driver_infrastructure.COMPLETED, []driver_infrastructure.ConnectRouting{},
		[]driver_infrastructure.ExecuteRouting{}, roleByHost, correspondingHosts)
	defer (&bg.BlueGreenPluginFactory{}).ClearCaches()

	t.Run("ClosingMethod", func(t *testing.T) {
		storage := newTestStorageService()
		defer storage.Stop()
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockContainer := setupBgServicesContainer(ctrl, mockPluginService, storage)
		// No BG status in storage
		mockPluginService.EXPECT().GetDialect().Return(&driver_infrastructure.MySQLDatabaseDialect{}).AnyTimes()
		mockPluginService.EXPECT().GetCurrentHostInfo().Return(&host_info_util.HostInfo{Host: "test-host"}, nil).AnyTimes()

		plugin, err := bg.NewBlueGreenPlugin(mockContainer, props)
		assert.NoError(t, err)

		result, result2, ok, err := plugin.Execute(mockConn, "Close", executeFunc)

		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "result", result)
		assert.Nil(t, result2)
	})

	t.Run("NoBlueGreenStatus", func(t *testing.T) {
		storage := newTestStorageService()
		defer storage.Stop()
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockContainer := setupBgServicesContainer(ctrl, mockPluginService, storage)

		plugin, err := bg.NewBlueGreenPlugin(mockContainer, props)
		assert.NoError(t, err)

		// No BG status in storage

		result, result2, ok, err := plugin.Execute(mockConn, "Query", executeFunc)

		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "result", result)
		assert.Nil(t, result2)
	})

	t.Run("ErrorGettingCurrentHost", func(t *testing.T) {
		storage := newTestStorageService()
		defer storage.Stop()
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockContainer := setupBgServicesContainer(ctrl, mockPluginService, storage)

		plugin, err := bg.NewBlueGreenPlugin(mockContainer, props)
		assert.NoError(t, err)

		driver_infrastructure.BlueGreenStatusStorageType.Set(storage, bgStatusCacheKey("test-bg-id"), &bgStatus)
		mockPluginService.EXPECT().GetCurrentHostInfo().Return((*host_info_util.HostInfo)(nil), errors.New("host error"))

		result, result2, ok, err := plugin.Execute(mockConn, "Query", executeFunc)

		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "result", result)
		assert.Nil(t, result2)
	})

	t.Run("NoMatchingHostRole", func(t *testing.T) {
		storage := newTestStorageService()
		defer storage.Stop()
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockContainer := setupBgServicesContainer(ctrl, mockPluginService, storage)

		plugin, err := bg.NewBlueGreenPlugin(mockContainer, props)
		assert.NoError(t, err)

		driver_infrastructure.BlueGreenStatusStorageType.Set(storage, bgStatusCacheKey("test-bg-id"), &bgStatus)
		mockPluginService.EXPECT().GetCurrentHostInfo().Return(hostInfo, nil)
		mockPluginService.EXPECT().GetDialect().Return(&driver_infrastructure.MySQLDatabaseDialect{}).AnyTimes()

		result, result2, ok, err := plugin.Execute(mockConn, "Query", executeFunc)

		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "result", result)
		assert.Nil(t, result2)
	})

	t.Run("NoMatchingRoutes", func(t *testing.T) {
		storage := newTestStorageService()
		defer storage.Stop()
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockContainer := setupBgServicesContainer(ctrl, mockPluginService, storage)
		roleByHost.Put(hostInfo.GetHost(), driver_infrastructure.TARGET)

		plugin, err := bg.NewBlueGreenPlugin(mockContainer, props)
		assert.NoError(t, err)

		driver_infrastructure.BlueGreenStatusStorageType.Set(storage, bgStatusCacheKey("test-bg-id"), &bgStatus)
		mockPluginService.EXPECT().GetCurrentHostInfo().Return(hostInfo, nil)
		mockPluginService.EXPECT().GetDialect().Return(&driver_infrastructure.MySQLDatabaseDialect{}).AnyTimes()

		result, result2, ok, err := plugin.Execute(mockConn, "Query", executeFunc)

		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "result", result)
		assert.Nil(t, result2)
	})

	t.Run("MatchingRoute", func(t *testing.T) {
		storage := newTestStorageService()
		defer storage.Stop()
		mockTelemetry := mock_telemetry.NewMockTelemetryFactory(ctrl)
		mockTelemetryCtx := mock_telemetry.NewMockTelemetryContext(ctrl)
		executeRouting := bg.NewSuspendExecuteRouting(hostInfo.GetHostAndPort(), driver_infrastructure.SOURCE, "test-bg-id", storage)
		bgStatusWithRouting := driver_infrastructure.NewBgStatus("test-bg-id", driver_infrastructure.COMPLETED, []driver_infrastructure.ConnectRouting{},
			[]driver_infrastructure.ExecuteRouting{executeRouting}, roleByHost, correspondingHosts)

		ctxBefore := context.Background()
		mockTelemetry.EXPECT().
			OpenTelemetryContext(gomock.Any(), telemetry.NESTED, ctxBefore).
			Return(mockTelemetryCtx, ctxBefore).AnyTimes()
		mockTelemetryCtx.EXPECT().CloseContext().AnyTimes()
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockContainer := setupBgServicesContainer(ctrl, mockPluginService, storage)
		roleByHost.Put(hostInfo.GetHost(), driver_infrastructure.SOURCE)

		// After Apply runs (which calls GetTelemetryContext), swap storage to bgStatus (no routing)
		// so the plugin loop exits on the next getBgStatus() call.
		telemetryCtxCallCount := 0
		mockPluginService.EXPECT().GetTelemetryContext().DoAndReturn(func() context.Context {
			telemetryCtxCallCount++
			if telemetryCtxCallCount > 1 {
				driver_infrastructure.BlueGreenStatusStorageType.Set(storage, bgStatusCacheKey("test-bg-id"), &bgStatus)
			}
			return ctxBefore
		}).AnyTimes()
		mockPluginService.EXPECT().GetTelemetryFactory().Return(mockTelemetry).AnyTimes()
		mockPluginService.EXPECT().SetTelemetryContext(gomock.Any()).AnyTimes()

		plugin, err := bg.NewBlueGreenPlugin(mockContainer, props)
		assert.NoError(t, err)

		driver_infrastructure.BlueGreenStatusStorageType.Set(storage, bgStatusCacheKey("test-bg-id"), &bgStatusWithRouting)
		mockPluginService.EXPECT().GetCurrentHostInfo().Return(hostInfo, nil)
		mockPluginService.EXPECT().GetDialect().Return(&driver_infrastructure.MySQLDatabaseDialect{}).AnyTimes()

		result, result2, ok, err := plugin.Execute(mockConn, "Query", executeFunc)

		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "result", result)
		assert.Nil(t, result2)
	})
}
