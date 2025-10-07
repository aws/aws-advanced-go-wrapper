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
	"testing"
	"time"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestConnectTimeFactoryReturnsConnectTimePlugin(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockService := mock_driver_infrastructure.NewMockPluginService(ctrl)

	factory := plugins.NewConnectTimePluginFactory()
	plugin, err := factory.GetInstance(mockService, emptyProps)
	assert.NoError(t, err)

	_, ok := plugin.(*plugins.ConnectTimePlugin)
	assert.True(t, ok)
}

func TestConnectTimePlugin_ConnectTracksTime(t *testing.T) {
	plugin := &plugins.ConnectTimePlugin{}

	connected := false
	mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		time.Sleep(10 * time.Millisecond)
		connected = true
		return nil, nil
	}

	conn, err := plugin.Connect(nil, emptyProps, true, mockConnectFunc)

	assert.True(t, connected)
	assert.Nil(t, conn)
	assert.NoError(t, err)

	connectTime := plugin.GetTotalConnectTime()
	assert.Greater(t, connectTime, int64(0))
}

func TestConnectTimePlugin_ForceConnectTracksTime(t *testing.T) {
	plugin := &plugins.ConnectTimePlugin{}

	connected := false
	mockForceConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		time.Sleep(10 * time.Millisecond)
		connected = true
		return nil, nil
	}

	conn, err := plugin.ForceConnect(nil, emptyProps, true, mockForceConnectFunc)

	assert.True(t, connected)
	assert.Nil(t, conn)
	assert.NoError(t, err)

	connectTime := plugin.GetTotalConnectTime()
	assert.Greater(t, connectTime, int64(0))
}

func TestConnectTimePlugin_ResetConnectTime(t *testing.T) {
	plugin := &plugins.ConnectTimePlugin{}
	mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		time.Sleep(5 * time.Millisecond)
		return nil, nil
	}

	_, err := plugin.Connect(nil, emptyProps, true, mockConnectFunc)
	assert.NoError(t, err)
	assert.Greater(t, plugin.GetTotalConnectTime(), int64(0))

	plugin.ResetConnectTime()
	assert.Equal(t, int64(0), plugin.GetTotalConnectTime())
}

func TestConnectTimePlugin_GetSubscribedMethods(t *testing.T) {
	plugin := &plugins.ConnectTimePlugin{}
	methods := plugin.GetSubscribedMethods()
	assert.Contains(t, methods, plugin_helpers.CONNECT_METHOD)
	assert.Contains(t, methods, plugin_helpers.FORCE_CONNECT_METHOD)
	assert.Len(t, methods, 2)
}

func TestConnectTimePlugin_AccumulatesTime(t *testing.T) {
	plugin := &plugins.ConnectTimePlugin{}
	mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		time.Sleep(5 * time.Millisecond)
		return nil, nil
	}

	_, err := plugin.Connect(nil, emptyProps, true, mockConnectFunc)
	assert.NoError(t, err)
	firstTime := plugin.GetTotalConnectTime()

	_, err = plugin.ForceConnect(nil, emptyProps, true, mockConnectFunc)
	assert.NoError(t, err)
	totalTime := plugin.GetTotalConnectTime()

	assert.Greater(t, totalTime, firstTime)
}
