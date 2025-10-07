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

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	mock_telemetry "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/util/telemetry"
	mock_database_sql_driver "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/database_sql_driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/bg"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlueGreenPluginFactory_GetInstance(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	factory := bg.NewBlueGreenPluginFactory()
	require.NotNil(t, factory)
	assert.IsType(t, &bg.BlueGreenPluginFactory{}, factory)

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)

	plugin, err := factory.GetInstance(mockPluginService, MakeMapFromKeysAndVals(
		property_util.BGD_ID.Name, "",
	))
	assert.Nil(t, plugin)
	require.NotNil(t, err)
	assert.Equal(t, error_util.GetMessage("BlueGreenDeployment.bgIdRequired"), err.Error())

	plugin, err = factory.GetInstance(mockPluginService, MakeMapFromKeysAndVals(
		property_util.BGD_ID.Name, "test-bg-id",
	))
	assert.NoError(t, err)
	assert.NotNil(t, plugin)
	assert.IsType(t, &bg.BlueGreenPlugin{}, plugin)
}

func TestBlueGreenPlugin_GetSubscribedMethods(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	props := MakeMapFromKeysAndVals(
		property_util.BGD_ID.Name, "test-bg-id",
	)

	plugin, err := bg.NewBlueGreenPlugin(mockPluginService, props)
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
	emptyStatus := driver_infrastructure.BlueGreenStatus{}
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
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockPluginService.EXPECT().GetBgStatus("test-bg-id").Return(emptyStatus, false)
		mockPluginService.EXPECT().GetDialect().Return(&driver_infrastructure.MySQLDatabaseDialect{}).AnyTimes()

		plugin, err := bg.NewBlueGreenPlugin(mockPluginService, props)
		assert.NoError(t, err)

		conn, err := plugin.Connect(hostInfo, props, true, connectFunc)

		assert.NoError(t, err)
		assert.Equal(t, mockConn, conn)
	})

	t.Run("InitialConnectionWithIAM", func(t *testing.T) {
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)

		plugin, err := bg.NewBlueGreenPlugin(mockPluginService, props)
		assert.NoError(t, err)

		mockPluginService.EXPECT().GetBgStatus("test-bg-id").Return(bgStatus, true)
		mockPluginService.EXPECT().IsPluginInUse(driver_infrastructure.IAM_PLUGIN_CODE).Return(true)
		mockPluginService.EXPECT().GetDialect().Return(&driver_infrastructure.MySQLDatabaseDialect{}).AnyTimes()

		conn, err := plugin.Connect(hostInfo, props, true, connectFunc)

		assert.NoError(t, err)
		assert.Equal(t, mockConn, conn)
	})

	t.Run("NoMatchingHostRole", func(t *testing.T) {
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)

		plugin, err := bg.NewBlueGreenPlugin(mockPluginService, props)
		assert.NoError(t, err)

		mockPluginService.EXPECT().GetBgStatus("test-bg-id").Return(bgStatus, true)
		mockPluginService.EXPECT().IsPluginInUse(driver_infrastructure.IAM_PLUGIN_CODE).Return(false)
		mockPluginService.EXPECT().GetDialect().Return(&driver_infrastructure.MySQLDatabaseDialect{}).AnyTimes()

		conn, err := plugin.Connect(hostInfo, props, true, connectFunc)

		assert.NoError(t, err)
		assert.Equal(t, mockConn, conn)
	})

	t.Run("NoMatchingRoutes", func(t *testing.T) {
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		roleByHost.Put(hostInfo.GetHost(), driver_infrastructure.TARGET)

		plugin, err := bg.NewBlueGreenPlugin(mockPluginService, props)
		assert.NoError(t, err)

		mockPluginService.EXPECT().GetBgStatus("test-bg-id").Return(bgStatus, true)
		mockPluginService.EXPECT().IsPluginInUse(driver_infrastructure.IAM_PLUGIN_CODE).Return(false)
		mockPluginService.EXPECT().GetDialect().Return(&driver_infrastructure.MySQLDatabaseDialect{}).AnyTimes()

		conn, err := plugin.Connect(hostInfo, props, true, connectFunc)

		assert.NoError(t, err)
		assert.Equal(t, mockConn, conn)
	})

	t.Run("RoutingConnects", func(t *testing.T) {
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		roleByHost.Put(hostInfo.GetHost(), driver_infrastructure.SOURCE)

		plugin, err := bg.NewBlueGreenPlugin(mockPluginService, props)
		assert.NoError(t, err)

		mockPluginService.EXPECT().GetBgStatus("test-bg-id").Return(bgStatus, true)
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
	emptyStatus := driver_infrastructure.BlueGreenStatus{}
	props := MakeMapFromKeysAndVals(
		property_util.BGD_ID.Name, "test-bg-id",
	)
	executeFunc := func() (any, any, bool, error) {
		return "result", nil, true, nil
	}
	roleByHost := utils.NewRWMap[string, driver_infrastructure.BlueGreenRole]()
	correspondingHosts := utils.NewRWMap[string, utils.Pair[*host_info_util.HostInfo, *host_info_util.HostInfo]]()
	hostInfo := &host_info_util.HostInfo{Host: "test-host"}
	executeRouting := bg.NewSuspendExecuteRouting(hostInfo.GetHostAndPort(), driver_infrastructure.SOURCE, "test-bg-id")
	bgStatus := driver_infrastructure.NewBgStatus("test-bg-id", driver_infrastructure.COMPLETED, []driver_infrastructure.ConnectRouting{},
		[]driver_infrastructure.ExecuteRouting{}, roleByHost, correspondingHosts)
	defer (&bg.BlueGreenPluginFactory{}).ClearCaches()

	t.Run("ClosingMethod", func(t *testing.T) {
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockPluginService.EXPECT().GetBgStatus("test-bg-id").Return(emptyStatus, false)
		mockPluginService.EXPECT().GetDialect().Return(&driver_infrastructure.MySQLDatabaseDialect{}).AnyTimes()
		mockPluginService.EXPECT().GetCurrentHostInfo().Return(&host_info_util.HostInfo{Host: "test-host"}, nil).AnyTimes()

		plugin, err := bg.NewBlueGreenPlugin(mockPluginService, props)
		assert.NoError(t, err)

		result, result2, ok, err := plugin.Execute(mockConn, "Close", executeFunc)

		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "result", result)
		assert.Nil(t, result2)
	})

	t.Run("NoBlueGreenStatus", func(t *testing.T) {
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)

		plugin, err := bg.NewBlueGreenPlugin(mockPluginService, props)
		assert.NoError(t, err)

		mockPluginService.EXPECT().GetBgStatus("test-bg-id").Return(emptyStatus, false)

		result, result2, ok, err := plugin.Execute(mockConn, "Query", executeFunc)

		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "result", result)
		assert.Nil(t, result2)
	})

	t.Run("ErrorGettingCurrentHost", func(t *testing.T) {
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)

		plugin, err := bg.NewBlueGreenPlugin(mockPluginService, props)
		assert.NoError(t, err)

		mockPluginService.EXPECT().GetBgStatus("test-bg-id").Return(bgStatus, true)
		mockPluginService.EXPECT().GetCurrentHostInfo().Return((*host_info_util.HostInfo)(nil), errors.New("host error"))

		result, result2, ok, err := plugin.Execute(mockConn, "Query", executeFunc)

		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "result", result)
		assert.Nil(t, result2)
	})

	t.Run("NoMatchingHostRole", func(t *testing.T) {
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)

		plugin, err := bg.NewBlueGreenPlugin(mockPluginService, props)
		assert.NoError(t, err)

		mockPluginService.EXPECT().GetBgStatus("test-bg-id").Return(bgStatus, true)
		mockPluginService.EXPECT().GetCurrentHostInfo().Return(hostInfo, nil)
		mockPluginService.EXPECT().GetDialect().Return(&driver_infrastructure.MySQLDatabaseDialect{}).AnyTimes()

		result, result2, ok, err := plugin.Execute(mockConn, "Query", executeFunc)

		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "result", result)
		assert.Nil(t, result2)
	})

	t.Run("NoMatchingRoutes", func(t *testing.T) {
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		roleByHost.Put(hostInfo.GetHost(), driver_infrastructure.TARGET)

		plugin, err := bg.NewBlueGreenPlugin(mockPluginService, props)
		assert.NoError(t, err)

		mockPluginService.EXPECT().GetBgStatus("test-bg-id").Return(bgStatus, true)
		mockPluginService.EXPECT().GetCurrentHostInfo().Return(hostInfo, nil)
		mockPluginService.EXPECT().GetDialect().Return(&driver_infrastructure.MySQLDatabaseDialect{}).AnyTimes()

		result, result2, ok, err := plugin.Execute(mockConn, "Query", executeFunc)

		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "result", result)
		assert.Nil(t, result2)
	})

	t.Run("MatchingRoute", func(t *testing.T) {
		mockTelemetry := mock_telemetry.NewMockTelemetryFactory(ctrl)
		mockTelemetryCtx := mock_telemetry.NewMockTelemetryContext(ctrl)
		bgStatusWithRouting := driver_infrastructure.NewBgStatus("test-bg-id", driver_infrastructure.COMPLETED, []driver_infrastructure.ConnectRouting{},
			[]driver_infrastructure.ExecuteRouting{executeRouting}, roleByHost, correspondingHosts)

		ctxBefore := context.Background()
		mockTelemetry.EXPECT().
			OpenTelemetryContext(gomock.Any(), telemetry.NESTED, ctxBefore).
			Return(mockTelemetryCtx, ctxBefore).AnyTimes()
		mockTelemetryCtx.EXPECT().CloseContext().AnyTimes()
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		roleByHost.Put(hostInfo.GetHost(), driver_infrastructure.SOURCE)
		mockPluginService.EXPECT().GetTelemetryContext().Return(ctxBefore).Times(1)
		mockPluginService.EXPECT().GetTelemetryFactory().Return(mockTelemetry).Times(1)
		mockPluginService.EXPECT().SetTelemetryContext(ctxBefore).Times(2)

		plugin, err := bg.NewBlueGreenPlugin(mockPluginService, props)
		assert.NoError(t, err)

		mockPluginService.EXPECT().GetBgStatus("test-bg-id").Return(bgStatusWithRouting, true).Times(2)
		mockPluginService.EXPECT().GetBgStatus("test-bg-id").Return(bgStatus, true).AnyTimes()
		mockPluginService.EXPECT().GetCurrentHostInfo().Return(hostInfo, nil)
		mockPluginService.EXPECT().GetDialect().Return(&driver_infrastructure.MySQLDatabaseDialect{}).AnyTimes()

		result, result2, ok, err := plugin.Execute(mockConn, "Query", executeFunc)

		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "result", result)
		assert.Nil(t, result2)
	})
}
