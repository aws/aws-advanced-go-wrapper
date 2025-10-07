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
	mock_database_sql_driver "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/database_sql_driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/read_write_splitting"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestNewReadWriteSplittingPlugin(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)

	props := MakeMapFromKeysAndVals(
		"someKey", "someVal",
	)

	plugin := read_write_splitting.NewReadWriteSplittingPlugin(mockPluginService, props)

	assert.NotNil(t, plugin)
}

func TestNewReadWriteSplittingPluginFactory(t *testing.T) {
	factory := read_write_splitting.NewReadWriteSplittingPluginFactory()
	assert.NotNil(t, factory)
}

func TestReadWriteSplittingPluginFactory_GetInstance(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	factory := read_write_splitting.NewReadWriteSplittingPluginFactory()

	props := MakeMapFromKeysAndVals("test", "value")
	instance, err := factory.GetInstance(mockPluginService, props)

	assert.NoError(t, err)
	assert.NotNil(t, instance)
}

func TestReadWriteSplittingPlugin_GetSubscribedMethods(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	plugin := read_write_splitting.NewReadWriteSplittingPlugin(mockPluginService, nil)

	methods := plugin.GetSubscribedMethods()
	assert.Contains(t, methods, plugin_helpers.CONNECT_METHOD)
	assert.Contains(t, methods, plugin_helpers.INIT_HOST_PROVIDER_METHOD)
	assert.Contains(t, methods, plugin_helpers.NOTIFY_CONNECTION_CHANGED_METHOD)
	assert.Contains(t, methods, utils.CONN_QUERY_CONTEXT)
	assert.Contains(t, methods, utils.CONN_EXEC_CONTEXT)
}

func TestReadWriteSplittingPlugin_InitHostProvider(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockHostProvider := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)

	plugin := read_write_splitting.NewReadWriteSplittingPlugin(mockPluginService, nil)

	called := false
	err := plugin.InitHostProvider(nil, mockHostProvider, func() error {
		called = true
		return nil
	})

	assert.NoError(t, err)
	assert.True(t, called)
}

func TestReadWriteSplittingPlugin_Connect_UnsupportedStrategy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	strategy := "unsupported"

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockPluginService.EXPECT().AcceptsStrategy(gomock.Any()).Return(false)

	props := emptyProps
	property_util.READER_HOST_SELECTOR_STRATEGY.Set(props, strategy)

	plugin := read_write_splitting.NewReadWriteSplittingPlugin(mockPluginService, props)

	conn, err := plugin.Connect(nil, nil, true, nil)

	assert.Nil(t, conn)
	assert.Error(t, err)
	assert.Equal(t,
		error_util.GetMessage("ReadWriteSplittingPlugin.unsupportedHostSelectorStrategy",
			strategy,
			property_util.READER_HOST_SELECTOR_STRATEGY.Name),
		err.Error())
}

func TestReadWriteSplittingPlugin_Connect_StaticProvider(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockHostProvider := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)
	mockPluginService.EXPECT().AcceptsStrategy(gomock.Any()).Return(true)
	mockHostProvider.EXPECT().IsStaticHostListProvider().Return(true)

	plugin := read_write_splitting.NewReadWriteSplittingPlugin(mockPluginService, nil)
	_ = plugin.InitHostProvider(nil, mockHostProvider, func() error { return nil })

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	resultConn := mockConn

	conn, err := plugin.Connect(nil, nil, true, func(_ *utils.RWMap[string, string]) (driver.Conn, error) {
		return resultConn, nil
	})

	assert.NoError(t, err)
	assert.Equal(t, resultConn, conn)
}

func TestReadWriteSplittingPlugin_Connect_UnknownHostRole(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockHostProvider := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)
	mockPluginService.EXPECT().AcceptsStrategy(gomock.Any()).Return(true)
	mockHostProvider.EXPECT().IsStaticHostListProvider().Return(false)
	mockPluginService.EXPECT().GetHostRole(gomock.Any()).Return(host_info_util.UNKNOWN)

	plugin := read_write_splitting.NewReadWriteSplittingPlugin(mockPluginService, nil)
	_ = plugin.InitHostProvider(nil, mockHostProvider, func() error { return nil })

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	resultConn := mockConn

	conn, err := plugin.Connect(nil, nil, true, func(_ *utils.RWMap[string, string]) (driver.Conn, error) {
		return resultConn, nil
	})

	assert.Error(t, err)
	assert.Nil(t, conn)
	assert.Equal(t,
		error_util.GetMessage("ReadWriteSplittingPlugin.errorVerifyingInitialHostRole"),
		err.Error())
}

func TestReadWriteSplittingPlugin_Connect_NilHostRole(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockHostProvider := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)
	mockPluginService.EXPECT().AcceptsStrategy(gomock.Any()).Return(true)
	mockHostProvider.EXPECT().IsStaticHostListProvider().Return(false)
	mockPluginService.EXPECT().GetHostRole(gomock.Any()).Return(host_info_util.READER)
	mockPluginService.EXPECT().GetInitialConnectionHostInfo().Return(nil)

	plugin := read_write_splitting.NewReadWriteSplittingPlugin(mockPluginService, nil)
	_ = plugin.InitHostProvider(nil, mockHostProvider, func() error { return nil })

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	resultConn := mockConn

	conn, err := plugin.Connect(nil, nil, true, func(_ *utils.RWMap[string, string]) (driver.Conn, error) {
		return resultConn, nil
	})

	assert.NoError(t, err)
	assert.Equal(t, mockConn, conn)
}

func TestReadWriteSplittingPlugin_Connect_SameHostRole(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockHostProvider := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)
	mockPluginService.EXPECT().AcceptsStrategy(gomock.Any()).Return(true)
	mockHostProvider.EXPECT().IsStaticHostListProvider().Return(false)
	mockPluginService.EXPECT().GetHostRole(gomock.Any()).Return(host_info_util.READER)
	mockPluginService.EXPECT().GetInitialConnectionHostInfo().Return(
		&host_info_util.HostInfo{Role: host_info_util.READER})

	plugin := read_write_splitting.NewReadWriteSplittingPlugin(mockPluginService, nil)
	_ = plugin.InitHostProvider(nil, mockHostProvider, func() error { return nil })

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	resultConn := mockConn

	conn, err := plugin.Connect(nil, nil, true, func(_ *utils.RWMap[string, string]) (driver.Conn, error) {
		return resultConn, nil
	})

	assert.NoError(t, err)
	assert.Equal(t, mockConn, conn)
}

func TestReadWriteSplittingPlugin_Connect_DifferentHostRole(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockHostProvider := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)
	mockPluginService.EXPECT().AcceptsStrategy(gomock.Any()).Return(true)
	mockHostProvider.EXPECT().IsStaticHostListProvider().Return(false)
	mockPluginService.EXPECT().GetHostRole(gomock.Any()).Return(host_info_util.WRITER)
	mockPluginService.EXPECT().GetInitialConnectionHostInfo().Return(
		&host_info_util.HostInfo{Role: host_info_util.READER})
	mockHostProvider.EXPECT().SetInitialConnectionHostInfo(gomock.Any()).Return()

	plugin := read_write_splitting.NewReadWriteSplittingPlugin(mockPluginService, nil)
	_ = plugin.InitHostProvider(nil, mockHostProvider, func() error { return nil })

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	resultConn := mockConn

	conn, err := plugin.Connect(nil, nil, true, func(_ *utils.RWMap[string, string]) (driver.Conn, error) {
		return resultConn, nil
	})

	assert.NoError(t, err)
	assert.Equal(t, mockConn, conn)
}

func TestReadWriteSplittingPlugin_NotifyConnectionChanged(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	plugin := read_write_splitting.NewReadWriteSplittingPlugin(mockPluginService, nil)
	mockPluginService.EXPECT().GetCurrentConnection()
	mockPluginService.EXPECT().GetCurrentHostInfo()

	// not in read/write split
	action := plugin.NotifyConnectionChanged(nil)
	assert.Equal(t, driver_infrastructure.NO_OPINION, action)
}

func TestReadWriteSplittingPlugin_Execute_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	plugin := read_write_splitting.NewReadWriteSplittingPlugin(mockPluginService, nil)
	mockDialect := mock_driver_infrastructure.NewMockDatabaseDialect(ctrl)

	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockDialect.EXPECT().DoesStatementSetReadOnly(gomock.Any()).Return(false, false).AnyTimes()

	executeFunc := func() (any, any, bool, error) { return nil, nil, false, nil }
	val1, val2, ok, err := plugin.Execute(nil, "QueryContext", executeFunc, "SELECT * FROM test")

	assert.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, val1)
	assert.Nil(t, val2)
}

func TestReadWriteSplittingPlugin_Execute_SwitchReadOnly(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	hostWriter1 := &host_info_util.HostInfo{Role: host_info_util.WRITER, Host: "writer1"}
	hostReader1 := &host_info_util.HostInfo{Role: host_info_util.READER, Host: "reader1"}
	hostReader2 := &host_info_util.HostInfo{Role: host_info_util.READER, Host: "reader2"}
	hosts := []*host_info_util.HostInfo{hostReader1, hostReader2, hostWriter1}

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	plugin := read_write_splitting.NewReadWriteSplittingPlugin(mockPluginService, nil)
	mockWriterConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockReaderConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockDatabaseDialect(ctrl)
	mockDriverDialect := mock_driver_infrastructure.NewMockDriverDialect(ctrl)

	// Setup common mocks
	mockPluginService.EXPECT().GetTargetDriverDialect().Return(mockDriverDialect).AnyTimes()
	mockDriverDialect.EXPECT().IsClosed(gomock.Any()).Return(false).AnyTimes()
	mockPluginService.EXPECT().RefreshHostList(gomock.Any()).Return(nil).AnyTimes()
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockDialect.EXPECT().DoesStatementSetReadOnly(gomock.Any()).Return(true, true).AnyTimes()
	mockPluginService.EXPECT().IsInTransaction().Return(false).AnyTimes()
	mockPluginService.EXPECT().GetHosts().Return(hosts).AnyTimes()

	// Connection flow expectations
	mockPluginService.EXPECT().GetCurrentConnection().Return(mockWriterConn).Times(3)
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(hostWriter1, nil).Times(2)
	mockPluginService.EXPECT().GetHostInfoByStrategy(gomock.Any(), gomock.Any(), gomock.Any()).Return(hostReader1, nil)
	mockPluginService.EXPECT().Connect(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockReaderConn, nil)
	mockPluginService.EXPECT().SetCurrentConnection(mockReaderConn, hostReader1, nil).Return(nil)

	executeFunc := func() (any, any, bool, error) { return nil, nil, false, nil }
	val1, val2, ok, err := plugin.Execute(nil, "QueryContext", executeFunc, "SELECT * FROM test")

	assert.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, val1)
	assert.Nil(t, val2)
}

func TestReadWriteSplittingPlugin_Execute_ReadOnlyNoReader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	hostWriter1 := &host_info_util.HostInfo{Role: host_info_util.WRITER, Host: "writer1"}
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	plugin := read_write_splitting.NewReadWriteSplittingPlugin(mockPluginService, nil)
	mockWriterConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockDatabaseDialect(ctrl)
	mockDriverDialect := mock_driver_infrastructure.NewMockDriverDialect(ctrl)

	// Setup mocks
	mockPluginService.EXPECT().GetTargetDriverDialect().Return(mockDriverDialect).AnyTimes()
	mockDriverDialect.EXPECT().IsClosed(gomock.Any()).Return(false).AnyTimes()
	mockPluginService.EXPECT().RefreshHostList(gomock.Any()).Return(nil).AnyTimes()
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockDialect.EXPECT().DoesStatementSetReadOnly(gomock.Any()).Return(true, true).AnyTimes()
	mockPluginService.EXPECT().IsInTransaction().Return(false).AnyTimes()
	mockPluginService.EXPECT().GetHosts().Return([]*host_info_util.HostInfo{hostWriter1}).AnyTimes()

	// Connection flow expectations
	mockPluginService.EXPECT().GetCurrentConnection().Return(mockWriterConn).Times(3)
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(hostWriter1, nil).Times(2)
	mockPluginService.EXPECT().Connect(hostWriter1, gomock.Any(), gomock.Any()).Return(mockWriterConn, nil)

	executeFunc := func() (any, any, bool, error) { return nil, nil, false, nil }
	val1, val2, ok, err := plugin.Execute(nil, "QueryContext", executeFunc, "SELECT * FROM test")

	assert.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, val1)
	assert.Nil(t, val2)
}

func TestReadWriteSplittingPlugin_Execute_SwitchWriter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	hostWriter1 := &host_info_util.HostInfo{Role: host_info_util.WRITER, Host: "writer1"}
	hostReader1 := &host_info_util.HostInfo{Role: host_info_util.READER, Host: "reader1"}
	hostReader2 := &host_info_util.HostInfo{Role: host_info_util.READER, Host: "reader2"}
	hosts := []*host_info_util.HostInfo{hostReader1, hostReader2, hostWriter1}

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	plugin := read_write_splitting.NewReadWriteSplittingPlugin(mockPluginService, nil)
	mockWriterConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockReaderConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockDatabaseDialect(ctrl)
	mockDriverDialect := mock_driver_infrastructure.NewMockDriverDialect(ctrl)

	// Setup mocks
	mockPluginService.EXPECT().GetTargetDriverDialect().Return(mockDriverDialect).AnyTimes()
	mockDriverDialect.EXPECT().IsClosed(gomock.Any()).Return(false).AnyTimes()
	mockPluginService.EXPECT().RefreshHostList(gomock.Any()).Return(nil).AnyTimes()
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockDialect.EXPECT().DoesStatementSetReadOnly(gomock.Any()).Return(false, true)
	mockPluginService.EXPECT().IsInTransaction().Return(false).AnyTimes()
	mockPluginService.EXPECT().GetHosts().Return(hosts).AnyTimes()

	// Connection flow expectations
	mockPluginService.EXPECT().GetCurrentConnection().Return(mockReaderConn).Times(3)
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(hostReader1, nil).Times(2)
	mockPluginService.EXPECT().Connect(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockWriterConn, nil)
	mockPluginService.EXPECT().SetCurrentConnection(mockWriterConn, hostWriter1, nil).Return(nil)

	executeFunc := func() (any, any, bool, error) { return nil, nil, false, nil }
	val1, val2, ok, err := plugin.Execute(nil, "QueryContext", executeFunc, "SELECT * FROM test")

	assert.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, val1)
	assert.Nil(t, val2)
}

func TestReadWriteSplittingPlugin_Execute_SwitchReaderWriterReaderThenClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	hostWriter1 := &host_info_util.HostInfo{Role: host_info_util.WRITER, Host: "writer1"}
	hostReader1 := &host_info_util.HostInfo{Role: host_info_util.READER, Host: "reader1"}
	hostReader2 := &host_info_util.HostInfo{Role: host_info_util.READER, Host: "reader2"}

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	plugin := read_write_splitting.NewReadWriteSplittingPlugin(mockPluginService, nil)
	mockWriterConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockReaderConn := mock_database_sql_driver.NewMockConn(ctrl)

	// Setup common mocks
	mockDialect := mock_driver_infrastructure.NewMockDatabaseDialect(ctrl)
	mockDriverDialect := mock_driver_infrastructure.NewMockDriverDialect(ctrl)

	mockPluginService.EXPECT().GetTargetDriverDialect().Return(mockDriverDialect).AnyTimes()
	mockPluginService.EXPECT().RefreshHostList(gomock.Any()).Return(nil).AnyTimes()
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().IsInTransaction().Return(false).AnyTimes()
	mockPluginService.EXPECT().GetHosts().Return([]*host_info_util.HostInfo{
		hostReader1, hostReader2, hostWriter1,
	}).AnyTimes()

	executeFunc := func() (any, any, bool, error) {
		return nil, nil, false, nil
	}

	// First execution: Switch to reader
	mockDialect.EXPECT().DoesStatementSetReadOnly(gomock.Any()).Return(true, true)
	mockDriverDialect.EXPECT().IsClosed(gomock.Any()).Return(false).Times(2)
	mockPluginService.EXPECT().GetCurrentConnection().Return(mockWriterConn).Times(3)
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(hostWriter1, nil).Times(2)
	mockPluginService.EXPECT().GetHostInfoByStrategy(gomock.Any(), gomock.Any(), gomock.Any()).Return(hostReader1, nil)
	mockPluginService.EXPECT().Connect(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockReaderConn, nil)
	mockPluginService.EXPECT().SetCurrentConnection(mockReaderConn, hostReader1, nil).Return(nil)

	val1, val2, ok, err := plugin.Execute(nil, "QueryContext", executeFunc, "SELECT * FROM test")
	assert.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, val1)
	assert.Nil(t, val2)

	// Second execution: Switch to writer
	mockDialect.EXPECT().DoesStatementSetReadOnly(gomock.Any()).Return(false, true)
	mockDriverDialect.EXPECT().IsClosed(gomock.Any()).Return(false).Times(2)
	mockPluginService.EXPECT().GetCurrentConnection().Return(mockReaderConn).Times(3)
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(hostReader1, nil).Times(2)
	mockPluginService.EXPECT().Connect(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockWriterConn, nil)
	mockPluginService.EXPECT().SetCurrentConnection(mockWriterConn, hostWriter1, nil).Return(nil)

	val1, val2, ok, err = plugin.Execute(nil, "QueryContext", executeFunc, "SELECT * FROM test")
	assert.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, val1)
	assert.Nil(t, val2)

	// Third execution: Switch back to reader
	mockDialect.EXPECT().DoesStatementSetReadOnly(gomock.Any()).Return(true, true)
	mockDriverDialect.EXPECT().IsClosed(gomock.Any()).Return(false).Times(3)
	mockPluginService.EXPECT().GetCurrentConnection().Return(mockWriterConn).Times(3)
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(hostWriter1, nil).Times(2)
	mockPluginService.EXPECT().SetCurrentConnection(mockReaderConn, hostReader1, nil).Return(nil)

	val1, val2, ok, err = plugin.Execute(nil, "QueryContext", executeFunc, "SELECT * FROM test")
	assert.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, val1)
	assert.Nil(t, val2)

	// Fourth execution: Close all connections by asserting connection closed error
	randomMockConnection := mock_database_sql_driver.NewMockConn(ctrl)
	mockDialect.EXPECT().DoesStatementSetReadOnly(gomock.Any()).Return(true, true)
	mockPluginService.EXPECT().GetCurrentConnection().Return(randomMockConnection).Times(3)
	mockDriverDialect.EXPECT().IsClosed(randomMockConnection).Return(true)
	mockDriverDialect.EXPECT().IsClosed(mockReaderConn).Return(false)
	mockReaderConn.EXPECT().Close()
	mockDriverDialect.EXPECT().IsClosed(mockWriterConn).Return(false)
	mockWriterConn.EXPECT().Close()

	val1, val2, ok, err = plugin.Execute(nil, "QueryContext", executeFunc, "SELECT * FROM test")
	assert.Error(t, err)
	assert.False(t, ok)
	assert.Nil(t, val1)
	assert.Nil(t, val2)
}

func TestReadWriteSplittingPlugin_InitHostProvider_AfterReaderSetup(t *testing.T) {
	// Switch read only
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	hostWriter1 := &host_info_util.HostInfo{Role: host_info_util.WRITER, Host: "writer1"}
	hostReader1 := &host_info_util.HostInfo{Role: host_info_util.READER, Host: "reader1"}
	hostReader2 := &host_info_util.HostInfo{Role: host_info_util.READER, Host: "reader2"}

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	plugin := read_write_splitting.NewReadWriteSplittingPlugin(mockPluginService, nil)
	mockWriterConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockReaderConn := mock_database_sql_driver.NewMockConn(ctrl)

	// Setup mocks
	mockDialect := mock_driver_infrastructure.NewMockDatabaseDialect(ctrl)
	mockDriverDialect := mock_driver_infrastructure.NewMockDriverDialect(ctrl)

	mockPluginService.EXPECT().GetTargetDriverDialect().Return(mockDriverDialect).AnyTimes()
	mockDriverDialect.EXPECT().IsClosed(gomock.Any()).Return(false).AnyTimes()
	mockPluginService.EXPECT().RefreshHostList(gomock.Any()).Return(nil).AnyTimes()
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockDialect.EXPECT().DoesStatementSetReadOnly(gomock.Any()).Return(true, true).AnyTimes()
	mockPluginService.EXPECT().IsInTransaction().Return(false).AnyTimes()
	mockPluginService.EXPECT().GetHosts().Return([]*host_info_util.HostInfo{
		hostReader1, hostReader2, hostWriter1,
	}).AnyTimes()

	// Connection flow expectations
	mockPluginService.EXPECT().GetCurrentConnection().Return(mockWriterConn).Times(3)
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(hostWriter1, nil).Times(2)
	mockPluginService.EXPECT().GetHostInfoByStrategy(gomock.Any(), gomock.Any(), gomock.Any()).Return(hostReader1, nil)
	mockPluginService.EXPECT().Connect(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockReaderConn, nil)
	mockPluginService.EXPECT().SetCurrentConnection(mockReaderConn, hostReader1, nil).Return(nil)
	executeFunc := func() (any, any, bool, error) {
		return nil, nil, false, nil
	}

	val1, val2, ok, err := plugin.Execute(nil, "QueryContext", executeFunc, "SELECT * FROM test")

	assert.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, val1)
	assert.Nil(t, val2)

	// test current conn being reader
	mockPluginService.EXPECT().GetCurrentConnection().Return(mockReaderConn)
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(hostReader1, nil)
	action := plugin.NotifyConnectionChanged(nil)
	assert.Equal(t, driver_infrastructure.PRESERVE, action)

	// test current conn being writer
	mockPluginService.EXPECT().GetCurrentConnection().Return(mockWriterConn)
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(hostWriter1, nil)
	action = plugin.NotifyConnectionChanged(nil)
	assert.Equal(t, driver_infrastructure.PRESERVE, action)
}

func TestReadWriteSplittingPlugin_FailToConnectToCachedReader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	hosts := []*host_info_util.HostInfo{
		{Role: host_info_util.READER, Host: "reader1"},
		{Role: host_info_util.READER, Host: "reader2"},
		{Role: host_info_util.WRITER, Host: "writer1"},
	}

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	plugin := read_write_splitting.NewReadWriteSplittingPlugin(mockPluginService, nil)
	mockWriterConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockReaderConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockNewReaderConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockDatabaseDialect(ctrl)
	mockDriverDialect := mock_driver_infrastructure.NewMockDriverDialect(ctrl)

	// Setup common mocks
	mockPluginService.EXPECT().GetTargetDriverDialect().Return(mockDriverDialect).AnyTimes()
	mockDriverDialect.EXPECT().IsClosed(gomock.Any()).Return(false).AnyTimes()
	mockPluginService.EXPECT().RefreshHostList(gomock.Any()).Return(nil).AnyTimes()
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockDialect.EXPECT().DoesStatementSetReadOnly(gomock.Any()).Return(true, true).AnyTimes()
	mockPluginService.EXPECT().IsInTransaction().Return(false).AnyTimes()
	mockPluginService.EXPECT().GetHosts().Return(hosts).AnyTimes()

	executeFunc := func() (any, any, bool, error) { return nil, nil, false, nil }

	// First execution: Establish reader connection
	mockPluginService.EXPECT().GetCurrentConnection().Return(mockWriterConn).Times(3)
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(hosts[2], nil).Times(2)
	mockPluginService.EXPECT().GetHostInfoByStrategy(gomock.Any(), gomock.Any(), gomock.Any()).Return(hosts[0], nil)
	mockPluginService.EXPECT().Connect(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockReaderConn, nil)
	mockPluginService.EXPECT().SetCurrentConnection(mockReaderConn, hosts[0], nil).Return(nil)

	val1, val2, ok, err := plugin.Execute(nil, "QueryContext", executeFunc, "SELECT * FROM test")
	assert.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, val1)
	assert.Nil(t, val2)

	// Second execution: Cached reader connection fails
	mockPluginService.EXPECT().GetCurrentConnection().Return(mockWriterConn).Times(4)
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(hosts[2], nil).Times(2)
	mockPluginService.EXPECT().SetCurrentConnection(mockReaderConn, hosts[0], nil).Return(errors.New("connection failed"))
	mockReaderConn.EXPECT().Close()
	mockPluginService.EXPECT().GetHostInfoByStrategy(gomock.Any(), gomock.Any(), gomock.Any()).Return(hosts[1], nil)
	mockPluginService.EXPECT().Connect(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockNewReaderConn, nil)
	mockPluginService.EXPECT().SetCurrentConnection(mockNewReaderConn, hosts[1], nil).Return(nil)

	val1, val2, ok, err = plugin.Execute(nil, "QueryContext", executeFunc, "SELECT * FROM test")
	assert.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, val1)
	assert.Nil(t, val2)
}

func TestReadWriteSplittingPlugin_NoWriterFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	hosts := []*host_info_util.HostInfo{
		{Role: host_info_util.READER, Host: "reader1"},
		{Role: host_info_util.READER, Host: "reader2"},
	}

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	plugin := read_write_splitting.NewReadWriteSplittingPlugin(mockPluginService, nil)
	mockReaderConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockDatabaseDialect(ctrl)
	mockDriverDialect := mock_driver_infrastructure.NewMockDriverDialect(ctrl)

	// Setup mocks
	mockPluginService.EXPECT().GetTargetDriverDialect().Return(mockDriverDialect).AnyTimes()
	mockDriverDialect.EXPECT().IsClosed(gomock.Any()).Return(false).AnyTimes()
	mockPluginService.EXPECT().RefreshHostList(gomock.Any()).Return(nil).AnyTimes()
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockDialect.EXPECT().DoesStatementSetReadOnly(gomock.Any()).Return(false, true)
	mockPluginService.EXPECT().IsInTransaction().Return(false).AnyTimes()
	mockPluginService.EXPECT().GetHosts().Return(hosts).AnyTimes()
	mockPluginService.EXPECT().GetCurrentConnection().Return(mockReaderConn).Times(4)
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(hosts[0], nil).Times(2)

	executeFunc := func() (any, any, bool, error) { return nil, nil, false, nil }
	val1, val2, ok, err := plugin.Execute(nil, "QueryContext", executeFunc, "INSERT INTO test VALUES (1)")

	assert.Error(t, err)
	assert.False(t, ok)
	assert.Nil(t, val1)
	assert.Nil(t, val2)
	assert.Contains(t, err.Error(), error_util.GetMessage("ReadWriteSplittingPlugin.noWriterFound"))
}

func TestReadWriteSplittingPlugin_WriterFallback(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	hosts := []*host_info_util.HostInfo{
		{Role: host_info_util.READER, Host: "reader1"},
		{Role: host_info_util.WRITER, Host: "writer1"},
	}

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	plugin := read_write_splitting.NewReadWriteSplittingPlugin(mockPluginService, nil)
	mockWriterConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockDatabaseDialect(ctrl)
	mockDriverDialect := mock_driver_infrastructure.NewMockDriverDialect(ctrl)

	// Setup mocks
	mockPluginService.EXPECT().GetTargetDriverDialect().Return(mockDriverDialect).AnyTimes()
	mockDriverDialect.EXPECT().IsClosed(gomock.Any()).Return(false).AnyTimes()
	mockPluginService.EXPECT().RefreshHostList(gomock.Any()).Return(nil).AnyTimes()
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockDialect.EXPECT().DoesStatementSetReadOnly(gomock.Any()).Return(true, true)
	mockPluginService.EXPECT().IsInTransaction().Return(false).AnyTimes()
	mockPluginService.EXPECT().GetHosts().Return(hosts).AnyTimes()
	mockPluginService.EXPECT().GetCurrentConnection().Return(mockWriterConn).Times(2)
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(hosts[1], nil).Times(2)
	mockPluginService.EXPECT().GetHostInfoByStrategy(gomock.Any(), gomock.Any(), gomock.Any()).Return(hosts[0], errors.New("no readers available")).AnyTimes()

	executeFunc := func() (any, any, bool, error) { return nil, nil, false, nil }
	val1, val2, ok, err := plugin.Execute(nil, "QueryContext", executeFunc, "SELECT * FROM test")

	assert.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, val1)
	assert.Nil(t, val2)
}

func TestReadWriteSplittingPlugin_FailoverError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	plugin := read_write_splitting.NewReadWriteSplittingPlugin(mockPluginService, nil)
	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockDatabaseDialect(ctrl)
	mockDriverDialect := mock_driver_infrastructure.NewMockDriverDialect(ctrl)

	// Setup mocks
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockDialect.EXPECT().DoesStatementSetReadOnly(gomock.Any()).Return(false, false).AnyTimes()
	mockPluginService.EXPECT().GetTargetDriverDialect().Return(mockDriverDialect).AnyTimes()
	mockDriverDialect.EXPECT().IsClosed(gomock.Any()).Return(false).AnyTimes()
	mockPluginService.EXPECT().GetCurrentConnection().Return(mockConn).Times(2)

	failoverError := &error_util.AwsWrapperError{ErrorType: error_util.FailoverSuccessError.ErrorType}
	executeFunc := func() (any, any, bool, error) { return nil, nil, false, failoverError }

	val1, val2, ok, err := plugin.Execute(nil, "QueryContext", executeFunc, "SELECT * FROM test")

	assert.Error(t, err)
	assert.False(t, ok)
	assert.Nil(t, val1)
	assert.Nil(t, val2)
	assert.Equal(t, failoverError, err)
}
