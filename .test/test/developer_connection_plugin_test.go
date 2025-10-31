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
	"database/sql/driver"
	"errors"
	"testing"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	mock_error_simulator "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/plugins/error_simulator"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/error_simulator"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

var (
	testError = errors.New("test")
	hostInfo  = &host_info_util.HostInfo{Host: "pg.testdb.us-east-2.rds.amazonaws.com", Port: 1234}
)

func TestDeveloperConnectionPlugin(t *testing.T) {
	t.Run("testRaiseError", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		error_simulator.ResetErrorSimulatorManager()

		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		properties := utils.NewRWMap[string, string]()
		properties.Put("plugins", "dev")
		plugin := plugins.NewDeveloperConnectionPlugin(mockPluginService, properties)
		mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
			return nil, nil
		}
		mockExecuteFunc := func() (any, any, bool, error) {
			return nil, nil, true, nil
		}

		// Should connect successfully
		_, err := plugin.Connect(hostInfo, properties, false, mockConnectFunc)
		assert.NoError(t, err)

		// Should execute successfully
		_, _, _, err = plugin.Execute(nil, "query", mockExecuteFunc)
		assert.NoError(t, err)

		// Raise error on next call
		devPlugin := plugin.(*plugins.DeveloperConnectionPlugin)
		devPlugin.RaiseErrorOnNextCall(testError, "")

		// Should fail with test error
		_, _, _, err = plugin.Execute(nil, "query", mockExecuteFunc)
		assert.Equal(t, testError, err)
	})

	t.Run("testRaiseErrorForMethodName", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		error_simulator.ResetErrorSimulatorManager()

		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		properties := utils.NewRWMap[string, string]()
		properties.Put("plugins", "dev")
		plugin := plugins.NewDeveloperConnectionPlugin(mockPluginService, properties)
		mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
			return nil, nil
		}
		mockExecuteFunc := func() (any, any, bool, error) {
			return nil, nil, true, nil
		}

		// Should connect successfully
		_, err := plugin.Connect(hostInfo, properties, false, mockConnectFunc)
		assert.NoError(t, err)

		// Should execute successfully
		_, _, _, err = plugin.Execute(nil, "query", mockExecuteFunc)
		assert.NoError(t, err)

		// Raise error on next call for specific method
		devPlugin := plugin.(*plugins.DeveloperConnectionPlugin)
		devPlugin.RaiseErrorOnNextCall(testError, "query")

		// Should fail with test error
		_, _, _, err = plugin.Execute(nil, "query", mockExecuteFunc)
		assert.Equal(t, testError, err)
	})

	t.Run("testRaiseErrorForAnyMethodName", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		error_simulator.ResetErrorSimulatorManager()

		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		properties := utils.NewRWMap[string, string]()
		properties.Put("plugins", "dev")
		plugin := plugins.NewDeveloperConnectionPlugin(mockPluginService, properties)
		mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
			return nil, nil
		}
		mockExecuteFunc := func() (any, any, bool, error) {
			return nil, nil, true, nil
		}

		// Should connect successfully
		_, err := plugin.Connect(hostInfo, properties, false, mockConnectFunc)
		assert.NoError(t, err)

		// Should execute successfully
		_, _, _, err = plugin.Execute(nil, "query", mockExecuteFunc)
		assert.NoError(t, err)

		// Raise error on next call for any method
		devPlugin := plugin.(*plugins.DeveloperConnectionPlugin)
		devPlugin.RaiseErrorOnNextCall(testError, "*")

		// Should fail with test error
		_, _, _, err = plugin.Execute(nil, "query", mockExecuteFunc)
		assert.Equal(t, testError, err)
	})

	t.Run("testRaiseErrorForWrongMethodName", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		error_simulator.ResetErrorSimulatorManager()

		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		properties := utils.NewRWMap[string, string]()
		properties.Put("plugins", "dev")
		plugin := plugins.NewDeveloperConnectionPlugin(mockPluginService, properties)
		mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
			return nil, nil
		}
		mockExecuteFunc := func() (any, any, bool, error) {
			return nil, nil, true, nil
		}

		// Should connect successfully
		_, err := plugin.Connect(hostInfo, properties, false, mockConnectFunc)
		assert.NoError(t, err)

		// Should execute successfully
		_, _, _, err = plugin.Execute(nil, "query", mockExecuteFunc)
		assert.NoError(t, err)

		// Raise error on next call for different method
		devPlugin := plugin.(*plugins.DeveloperConnectionPlugin)
		devPlugin.RaiseErrorOnNextCall(testError, "close")

		// Should execute successfully (wrong method name)
		_, _, _, err = plugin.Execute(nil, "query", mockExecuteFunc)
		assert.NoError(t, err)
	})

	t.Run("testRaiseErrorWithCallback", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		error_simulator.ResetErrorSimulatorManager()

		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		properties := utils.NewRWMap[string, string]()
		properties.Put("plugins", "dev")
		plugin := plugins.NewDeveloperConnectionPlugin(mockPluginService, properties)
		mockMethodCallback := mock_error_simulator.NewMockErrorSimulatorMethodCallback(ctrl)
		mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
			return nil, nil
		}
		mockExecuteFunc := func() (any, any, bool, error) {
			return nil, nil, true, nil
		}

		devPlugin := plugin.(*plugins.DeveloperConnectionPlugin)
		devPlugin.SetCallback(mockMethodCallback)

		mockArgs := []any{"test", "employees"}
		mockMethodCallback.EXPECT().GetErrorToRaise("query", gomock.Any()).Return(testError)
		mockMethodCallback.EXPECT().GetErrorToRaise("query", gomock.Any()).Return(nil)

		// Should connect successfully
		_, err := plugin.Connect(hostInfo, properties, false, mockConnectFunc)
		assert.NoError(t, err)

		// Should fail with test error
		_, _, _, err = plugin.Execute(nil, "query", mockExecuteFunc, mockArgs...)
		assert.Equal(t, testError, err)

		// Should execute successfully with different args
		_, _, _, err = plugin.Execute(nil, "query", mockExecuteFunc, "test", "admin")
		assert.NoError(t, err)
	})

	t.Run("testRaiseNoErrorWithCallback", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		error_simulator.ResetErrorSimulatorManager()

		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		properties := utils.NewRWMap[string, string]()
		properties.Put("plugins", "dev")
		plugin := plugins.NewDeveloperConnectionPlugin(mockPluginService, properties)
		mockMethodCallback := mock_error_simulator.NewMockErrorSimulatorMethodCallback(ctrl)
		mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
			return nil, nil
		}
		mockExecuteFunc := func() (any, any, bool, error) {
			return nil, nil, true, nil
		}

		devPlugin := plugin.(*plugins.DeveloperConnectionPlugin)
		devPlugin.SetCallback(mockMethodCallback)

		mockArgs := []any{"test", "employees"}
		mockMethodCallback.EXPECT().GetErrorToRaise("close", gomock.Any()).Return(nil).Times(2)

		// Should connect successfully
		_, err := plugin.Connect(hostInfo, properties, false, mockConnectFunc)
		assert.NoError(t, err)

		// Should execute successfully (different method)
		_, _, _, err = plugin.Execute(nil, "close", mockExecuteFunc, mockArgs...)
		assert.NoError(t, err)

		// Should execute successfully
		_, _, _, err = plugin.Execute(nil, "close", mockExecuteFunc, "test", "admin")
		assert.NoError(t, err)
	})

	t.Run("testRaiseErrorOnConnect", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		error_simulator.ResetErrorSimulatorManager()

		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		properties := utils.NewRWMap[string, string]()
		properties.Put("plugins", "dev")
		plugin := plugins.NewDeveloperConnectionPlugin(mockPluginService, properties)
		mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
			return nil, nil
		}

		errorSimulatorManager := error_simulator.GetErrorSimulatorManager()
		errorSimulatorManager.RaiseErrorOnNextConnect(testError)

		// Should fail with test error
		_, err := plugin.Connect(hostInfo, properties, false, mockConnectFunc)
		assert.Equal(t, testError, err)

		// Should connect successfully on second attempt
		_, err = plugin.Connect(hostInfo, properties, false, mockConnectFunc)
		assert.NoError(t, err)
	})

	t.Run("testNoErrorOnConnectWithCallback", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		error_simulator.ResetErrorSimulatorManager()

		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		properties := utils.NewRWMap[string, string]()
		properties.Put("plugins", "dev")
		plugin := plugins.NewDeveloperConnectionPlugin(mockPluginService, properties)
		mockConnectCallback := mock_error_simulator.NewMockErrorSimulatorConnectCallback(ctrl)
		mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
			return nil, nil
		}

		errorSimulatorManager := error_simulator.GetErrorSimulatorManager()
		errorSimulatorManager.SetCallback(mockConnectCallback)

		mockConnectCallback.EXPECT().GetErrorToRaise(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

		// Should connect successfully
		_, err := plugin.Connect(hostInfo, properties, false, mockConnectFunc)
		assert.NoError(t, err)
	})

	t.Run("testRaiseErrorOnConnectWithCallback", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		error_simulator.ResetErrorSimulatorManager()

		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		properties := utils.NewRWMap[string, string]()
		properties.Put("plugins", "dev")
		plugin := plugins.NewDeveloperConnectionPlugin(mockPluginService, properties)
		mockConnectCallback := mock_error_simulator.NewMockErrorSimulatorConnectCallback(ctrl)
		mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
			return nil, nil
		}

		errorSimulatorManager := error_simulator.GetErrorSimulatorManager()
		errorSimulatorManager.SetCallback(mockConnectCallback)

		gomock.InOrder(
			mockConnectCallback.EXPECT().GetErrorToRaise(gomock.Any(), gomock.Any(), gomock.Any()).Return(testError),
			mockConnectCallback.EXPECT().GetErrorToRaise(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil),
		)

		// Should fail with test error
		_, err := plugin.Connect(hostInfo, properties, false, mockConnectFunc)
		assert.Equal(t, testError, err)

		// Should connect successfully on second attempt
		_, err = plugin.Connect(hostInfo, properties, false, mockConnectFunc)
		assert.NoError(t, err)
	})
}
