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
	"errors"
	"testing"

	"database/sql/driver"

	mock_database_sql_driver "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/database_sql_driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/stretchr/testify/assert"
)

func TestBaseConnectionPlugin_GetSubscribedMethods(t *testing.T) {
	plugin := plugins.BaseConnectionPlugin{}
	methods := plugin.GetSubscribedMethods()
	assert.Empty(t, methods)
}

func TestBaseConnectionPlugin_Execute_CallsExecuteFunc(t *testing.T) {
	plugin := plugins.BaseConnectionPlugin{}

	expectedReturn1 := "value1"
	expectedReturn2 := "value2"
	expectedErr := error(nil)

	mockFunc := func() (any, any, bool, error) {
		return expectedReturn1, expectedReturn2, true, expectedErr
	}

	r1, r2, ok, err := plugin.Execute(nil, "TestMethod", mockFunc)
	assert.Equal(t, expectedReturn1, r1)
	assert.Equal(t, expectedReturn2, r2)
	assert.True(t, ok)
	assert.Equal(t, expectedErr, err)
}

func TestBaseConnectionPlugin_Connect_DelegatesToConnectFunc(t *testing.T) {
	plugin := plugins.BaseConnectionPlugin{}

	props := MakeMapFromKeysAndVals("key", "value")
	hostInfo := &host_info_util.HostInfo{}

	expectedConn := &mock_database_sql_driver.MockConn{}
	expectedErr := error(nil)

	connectFunc := func(p *utils.RWMap[string]) (driver.Conn, error) {
		assert.Equal(t, props, p)
		return expectedConn, expectedErr
	}

	conn, err := plugin.Connect(hostInfo, props, true, connectFunc)
	assert.Equal(t, expectedConn, conn)
	assert.NoError(t, err)
}

func TestBaseConnectionPlugin_ForceConnect_DelegatesToConnectFunc(t *testing.T) {
	plugin := plugins.BaseConnectionPlugin{}

	props := MakeMapFromKeysAndVals("key", "value")
	hostInfo := &host_info_util.HostInfo{}

	expectedConn := &mock_database_sql_driver.MockConn{}
	expectedErr := error(nil)

	connectFunc := func(p *utils.RWMap[string]) (driver.Conn, error) {
		assert.Equal(t, props, p)
		return expectedConn, expectedErr
	}

	conn, err := plugin.ForceConnect(hostInfo, props, false, connectFunc)
	assert.Equal(t, expectedConn, conn)
	assert.NoError(t, err)
}

func TestBaseConnectionPlugin_AcceptsStrategy_ReturnsFalse(t *testing.T) {
	plugin := plugins.BaseConnectionPlugin{}
	assert.False(t, plugin.AcceptsStrategy("any-strategy"))
}

func TestBaseConnectionPlugin_GetHostInfoByStrategy_ReturnsUnsupportedMethodError(t *testing.T) {
	plugin := plugins.BaseConnectionPlugin{}
	hostRole := host_info_util.WRITER
	hosts := []*host_info_util.HostInfo{}

	hostInfo, err := plugin.GetHostInfoByStrategy(hostRole, "strategy", hosts)

	assert.Nil(t, hostInfo)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "GetHostInfoByStrategy")
	assert.Contains(t, err.Error(), "BaseConnectionPlugin")
}

func TestBaseConnectionPlugin_GetHostSelectorStrategy_ReturnsUnsupportedMethodError(t *testing.T) {
	plugin := plugins.BaseConnectionPlugin{}
	selector, err := plugin.GetHostSelectorStrategy("strategy")

	assert.Nil(t, selector)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "GetHostSelectorStrategy")
	assert.Contains(t, err.Error(), "BaseConnectionPlugin")
}

func TestBaseConnectionPlugin_NotifyConnectionChanged_ReturnsNoOpinion(t *testing.T) {
	plugin := plugins.BaseConnectionPlugin{}
	action := plugin.NotifyConnectionChanged(map[driver_infrastructure.HostChangeOptions]bool{})
	assert.Equal(t, driver_infrastructure.NO_OPINION, action)
}

func TestBaseConnectionPlugin_NotifyHostListChanged_DoesNothing(t *testing.T) {
	plugin := plugins.BaseConnectionPlugin{}
	plugin.NotifyHostListChanged(map[string]map[driver_infrastructure.HostChangeOptions]bool{})
	// No panic or error expected, nothing to assert
}

func TestBaseConnectionPlugin_InitHostProvider_CallsInitHostProviderFunc(t *testing.T) {
	plugin := plugins.BaseConnectionPlugin{}

	called := false
	initFunc := func() error {
		called = true
		return nil
	}

	err := plugin.InitHostProvider(emptyProps, nil, initFunc)
	assert.True(t, called)
	assert.NoError(t, err)
}

func TestBaseConnectionPlugin_InitHostProvider_PropagatesError(t *testing.T) {
	plugin := plugins.BaseConnectionPlugin{}

	expectedErr := errors.New("init error")
	initFunc := func() error {
		return expectedErr
	}

	err := plugin.InitHostProvider(emptyProps, nil, initFunc)
	assert.ErrorIs(t, err, expectedErr)
}
