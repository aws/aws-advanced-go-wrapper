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
	"time"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestFactoryReturnsExecutionTimePlugin(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockService := mock_driver_infrastructure.NewMockPluginService(ctrl)

	factory := plugins.NewExecutionTimePluginFactory()
	plugin, err := factory.GetInstance(mockService, map[string]string{})
	assert.NoError(t, err)

	_, ok := plugin.(*plugins.ExecutionTimePlugin)
	assert.True(t, ok)
}

func TestExecutionTimePlugin_ExecuteTracksTime(t *testing.T) {
	plugin := &plugins.ExecutionTimePlugin{}

	executed := false
	mockFunc := func() (any, any, bool, error) {
		time.Sleep(10 * time.Millisecond) // simulate work
		executed = true
		return "result1", "result2", true, nil
	}

	ret1, ret2, ok, err := plugin.Execute(nil, "MockMethod", mockFunc)

	assert.True(t, executed)
	assert.Equal(t, "result1", ret1)
	assert.Equal(t, "result2", ret2)
	assert.True(t, ok)
	assert.NoError(t, err)

	execTime := plugin.GetTotalExecutionTime()
	assert.Greater(t, execTime, int64(0))
}

func TestExecutionTimePlugin_ResetExecutionTime(t *testing.T) {
	plugin := &plugins.ExecutionTimePlugin{}
	plugin.Execute(nil, "MockMethod", func() (any, any, bool, error) {
		time.Sleep(5 * time.Millisecond)
		return nil, nil, true, nil
	})
	assert.Greater(t, plugin.GetTotalExecutionTime(), int64(0))

	plugin.ResetExecutionTime()
	assert.Equal(t, int64(0), plugin.GetTotalExecutionTime())
}

func TestExecutionTimePlugin_GetSubscribedMethods(t *testing.T) {
	plugin := &plugins.ExecutionTimePlugin{}
	methods := plugin.GetSubscribedMethods()
	assert.Contains(t, methods, plugin_helpers.ALL_METHODS)
}
