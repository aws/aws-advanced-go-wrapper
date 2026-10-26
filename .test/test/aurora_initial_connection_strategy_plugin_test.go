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
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/plugins"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestAuroraInitialConnectionStrategyPlugin_InitHostProvider(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()
	mockHostListProviderService := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)

	initHostProviderFuncCalled := false
	mockInitHostProviderFunc := func() error {
		initHostProviderFuncCalled = true
		return nil
	}
	props := utils.NewRWMap[string, string]()

	plugin, err0 := plugins.NewAuroraInitialConnectionStrategyPlugin(mockContainer, props)
	assert.NoError(t, err0)

	err1 := plugin.InitHostProvider(props, mockHostListProviderService, mockInitHostProviderFunc)

	assert.NoError(t, err1)
	assert.True(t, initHostProviderFuncCalled)
}

func TestAuroraInitialConnectionStrategyPlugin_Connect_WithNonRdsClusterUrl(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()
	props := utils.NewRWMap[string, string]()
	hostInfo, err1 := host_info_util.NewHostInfoBuilder().
		SetHost("someNonRdsClusterUrl.someClusterId.amazon.com").
		Build()

	mockConnectFuncCalled := false
	mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		mockConnectFuncCalled = true
		return nil, nil
	}

	plugin, err0 := plugins.NewAuroraInitialConnectionStrategyPlugin(mockContainer, props)

	_, err2 := plugin.Connect(hostInfo, props, true, mockConnectFunc)

	assert.NoError(t, err0)
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.True(t, mockConnectFuncCalled)
}

// A CNAME in front of a cluster endpoint classifies as OTHER, where verifyInitialConnectionType was
// unreachable.
func TestAuroraInitialConnectionStrategyPlugin_Connect_WithCustomDomain_AndEmptyTopology_AndVerifyWriter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHostListProviderService := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)
	mockInitHostProviderFunc := func() error {
		return nil
	}
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()

	hostWriter1 := &host_info_util.HostInfo{Role: host_info_util.WRITER, Host: "instance-1.my-company.com"}
	mockPluginService.EXPECT().GetAllHosts().Return([]*host_info_util.HostInfo{})

	props := MakeMapFromKeysAndVals(
		property_util.VERIFY_INITIAL_CONNECTION_TYPE.Name, "writer",
		property_util.CLUSTER_INSTANCE_HOST_PATTERN.Name, "?.my-company.com")

	mockDriverConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		return mockDriverConn, nil
	}
	mockPluginService.EXPECT().ForceRefreshHostList(mockDriverConn).Return(nil)
	mockPluginService.EXPECT().IdentifyConnection(mockDriverConn).Return(hostWriter1, nil)
	mockHostListProviderService.EXPECT().SetInitialConnectionHostInfo(hostWriter1)

	plugin, err0 := plugins.NewAuroraInitialConnectionStrategyPlugin(mockContainer, props)
	_ = plugin.InitHostProvider(props, mockHostListProviderService, mockInitHostProviderFunc)

	hostInfo, err1 := host_info_util.NewHostInfoBuilder().SetHost("db.my-company.com").Build()
	actualConn, err2 := plugin.Connect(hostInfo, props, true, mockConnectFunc)

	assert.NoError(t, err0)
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.Equal(t, mockDriverConn, actualConn)
}

// Without the fast failure this is a 30 second stall ending in an error that names the wrong setting.
func TestAuroraInitialConnectionStrategyPlugin_Connect_WithCustomDomain_AndVerifyWriter_NoHostPattern(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()

	props := MakeMapFromKeysAndVals(property_util.VERIFY_INITIAL_CONNECTION_TYPE.Name, "writer")

	mockConnectFuncCalled := false
	mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		mockConnectFuncCalled = true
		return nil, nil
	}

	plugin, err0 := plugins.NewAuroraInitialConnectionStrategyPlugin(mockContainer, props)
	hostInfo, err1 := host_info_util.NewHostInfoBuilder().SetHost("db.my-company.com").Build()
	actualConn, err2 := plugin.Connect(hostInfo, props, true, mockConnectFunc)

	assert.NoError(t, err0)
	assert.NoError(t, err1)
	assert.Error(t, err2)
	assert.Contains(t, err2.Error(), property_util.CLUSTER_INSTANCE_HOST_PATTERN.Name)
	assert.Nil(t, actualConn)
	assert.False(t, mockConnectFuncCalled)
}

// Pins the deliberate exclusion: verifying a proxy endpoint would substitute the host and bypass it.
func TestAuroraInitialConnectionStrategyPlugin_Connect_WithRdsProxy_AndVerifyWriter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()

	props := MakeMapFromKeysAndVals(property_util.VERIFY_INITIAL_CONNECTION_TYPE.Name, "writer")

	mockDriverConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockConnectFuncCalled := false
	mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		mockConnectFuncCalled = true
		return mockDriverConn, nil
	}

	plugin, err0 := plugins.NewAuroraInitialConnectionStrategyPlugin(mockContainer, props)
	hostInfo, err1 := host_info_util.NewHostInfoBuilder().
		SetHost("database-test-name.proxy-XYZ.us-east-2.rds.amazonaws.com").
		Build()
	actualConn, err2 := plugin.Connect(hostInfo, props, true, mockConnectFunc)

	assert.NoError(t, err0)
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.Equal(t, mockDriverConn, actualConn)
	assert.True(t, mockConnectFuncCalled)
}

// A typo means "no verification", which is warned about but must keep connecting as it did before.
func TestAuroraInitialConnectionStrategyPlugin_InvalidVerifyInitialConnectionType(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()

	props := MakeMapFromKeysAndVals(property_util.VERIFY_INITIAL_CONNECTION_TYPE.Name, "wrtier")

	mockDriverConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		return mockDriverConn, nil
	}

	plugin, err0 := plugins.NewAuroraInitialConnectionStrategyPlugin(mockContainer, props)
	hostInfo, err1 := host_info_util.NewHostInfoBuilder().SetHost("db.example.com").Build()
	actualConn, err2 := plugin.Connect(hostInfo, props, true, mockConnectFunc)

	assert.NoError(t, err0)
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.Equal(t, mockDriverConn, actualConn)
}

// "none" declines verification even on a cluster endpoint, whose role is otherwise taken from the name.
func TestAuroraInitialConnectionStrategyPlugin_Connect_WithRdsWriterCluster_AndVerificationDeclined(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()

	props := MakeMapFromKeysAndVals(property_util.VERIFY_INITIAL_CONNECTION_TYPE.Name, "none")

	mockDriverConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockConnectFuncCalled := false
	mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		mockConnectFuncCalled = true
		return mockDriverConn, nil
	}

	plugin, err0 := plugins.NewAuroraInitialConnectionStrategyPlugin(mockContainer, props)
	hostInfo, err1 := host_info_util.NewHostInfoBuilder().
		SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").
		Build()
	actualConn, err2 := plugin.Connect(hostInfo, props, true, mockConnectFunc)

	assert.NoError(t, err0)
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.Equal(t, mockDriverConn, actualConn)
	assert.True(t, mockConnectFuncCalled)
}

func TestAuroraInitialConnectionStrategyPlugin_Connect_WithRdsWriterCluster(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHostListProviderService := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)
	mockInitHostProviderFunc := func() error {
		return nil
	}
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()

	hostWriter1 := &host_info_util.HostInfo{Role: host_info_util.WRITER, Host: "writer1"}
	hostReader1 := &host_info_util.HostInfo{Role: host_info_util.READER, Host: "reader1"}
	hostReader2 := &host_info_util.HostInfo{Role: host_info_util.READER, Host: "reader2"}
	hosts := []*host_info_util.HostInfo{hostReader1, hostReader2, hostWriter1}
	mockPluginService.EXPECT().GetAllHosts().Return(hosts)
	mockHostListProviderService.EXPECT().SetInitialConnectionHostInfo(hostWriter1)

	props := MakeMapFromKeysAndVals(property_util.VERIFY_INITIAL_CONNECTION_TYPE.Name, "writer")
	hostInfo, err0 := host_info_util.NewHostInfoBuilder().
		SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").
		Build()

	mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		return nil, nil
	}

	plugin, err1 := plugins.NewAuroraInitialConnectionStrategyPlugin(mockContainer, props)

	_ = plugin.InitHostProvider(props, mockHostListProviderService, mockInitHostProviderFunc)

	mockDriverConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockPluginService.EXPECT().Connect(hostWriter1, props, plugin).Return(mockDriverConn, nil)
	mockPluginService.EXPECT().GetHostRole(mockDriverConn).Return(host_info_util.WRITER)

	actualConn, err2 := plugin.Connect(hostInfo, props, true, mockConnectFunc)

	assert.NoError(t, err0)
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.Equal(t, mockDriverConn, actualConn)
}

func TestAuroraInitialConnectionStrategyPlugin_Connect_WithRdsWriterCluster_AndOutdatedTopology(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHostListProviderService := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)
	mockInitHostProviderFunc := func() error {
		return nil
	}
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()

	hostWriter1 := &host_info_util.HostInfo{Role: host_info_util.WRITER, Host: "writer1"}
	hosts := []*host_info_util.HostInfo{}
	mockPluginService.EXPECT().GetAllHosts().Return(hosts)

	props := MakeMapFromKeysAndVals(property_util.VERIFY_INITIAL_CONNECTION_TYPE.Name, "writer")

	mockDriverConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		return mockDriverConn, nil
	}
	mockPluginService.EXPECT().ForceRefreshHostList(mockDriverConn).Return(nil)
	mockPluginService.EXPECT().IdentifyConnection(mockDriverConn).Return(hostWriter1, nil)
	mockHostListProviderService.EXPECT().SetInitialConnectionHostInfo(hostWriter1)

	plugin, err0 := plugins.NewAuroraInitialConnectionStrategyPlugin(mockContainer, props)

	_ = plugin.InitHostProvider(props, mockHostListProviderService, mockInitHostProviderFunc)

	hostInfo, err1 := host_info_util.NewHostInfoBuilder().
		SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").
		Build()
	actualConn, err2 := plugin.Connect(hostInfo, props, true, mockConnectFunc)

	assert.NoError(t, err0)
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.Equal(t, mockDriverConn, actualConn)
}

func TestAuroraInitialConnectionStrategyPlugin_Connect_WithRdsWriterCluster_AndRetryAfterError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHostListProviderService := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)
	mockInitHostProviderFunc := func() error {
		return nil
	}
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()

	hostWriter1 := &host_info_util.HostInfo{Role: host_info_util.WRITER, Host: "writer1"}
	hostReader1 := &host_info_util.HostInfo{Role: host_info_util.READER, Host: "reader1"}
	hostReader2 := &host_info_util.HostInfo{Role: host_info_util.READER, Host: "reader2"}
	hosts := []*host_info_util.HostInfo{hostReader1, hostReader2, hostWriter1}
	mockPluginService.EXPECT().GetAllHosts().Return(hosts).MinTimes(5)

	props := MakeMapFromKeysAndVals(
		property_util.VERIFY_INITIAL_CONNECTION_TYPE.Name, "writer",
		property_util.INITIAL_CONNECTION_RETRY_INTERVAL_MS.Name, "10",
		property_util.INITIAL_CONNECTION_RETRY_TIMEOUT_MS.Name, "100")
	hostInfo, err0 := host_info_util.NewHostInfoBuilder().
		SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").
		Build()

	mockError := errors.New("someError")
	mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		return nil, mockError
	}

	plugin, err1 := plugins.NewAuroraInitialConnectionStrategyPlugin(mockContainer, props)

	_ = plugin.InitHostProvider(props, mockHostListProviderService, mockInitHostProviderFunc)

	mockDriverConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockErr := errors.New("someError")
	mockPluginService.EXPECT().Connect(hostWriter1, props, plugin).Return(mockDriverConn, mockErr).MinTimes(5)
	mockPluginService.EXPECT().IsLoginError(mockErr).Return(false).MinTimes(5)
	mockPluginService.EXPECT().SetAvailability(gomock.Any(), host_info_util.UNAVAILABLE).MinTimes(5)
	mockDriverConn.EXPECT().Close().MinTimes(2)

	actualConn, err2 := plugin.Connect(hostInfo, props, true, mockConnectFunc)

	assert.NoError(t, err0)
	assert.NoError(t, err1)
	assert.Error(t, mockError, err2)
	assert.Equal(t, nil, actualConn)
}

func TestAuroraInitialConnectionStrategyPlugin_Connect_WithRdsWriterCluster_AndRetryAfterWriterNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHostListProviderService := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)
	mockInitHostProviderFunc := func() error {
		return nil
	}
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()

	hostWriter1 := &host_info_util.HostInfo{Role: host_info_util.WRITER, Host: "writer1"}
	hostReader1 := &host_info_util.HostInfo{Role: host_info_util.READER, Host: "reader1"}
	hostReader2 := &host_info_util.HostInfo{Role: host_info_util.READER, Host: "reader2"}
	hosts := []*host_info_util.HostInfo{hostReader1, hostReader2, hostWriter1}
	mockPluginService.EXPECT().GetAllHosts().Return(hosts).MinTimes(5)

	props := MakeMapFromKeysAndVals(
		property_util.VERIFY_INITIAL_CONNECTION_TYPE.Name, "writer",
		property_util.INITIAL_CONNECTION_RETRY_INTERVAL_MS.Name, "10",
		property_util.INITIAL_CONNECTION_RETRY_TIMEOUT_MS.Name, "100")

	hostInfo, err0 := host_info_util.NewHostInfoBuilder().
		SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").
		Build()

	mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		return nil, nil
	}

	plugin, err1 := plugins.NewAuroraInitialConnectionStrategyPlugin(mockContainer, props)

	_ = plugin.InitHostProvider(props, mockHostListProviderService, mockInitHostProviderFunc)

	mockDriverConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockPluginService.EXPECT().Connect(hostWriter1, props, plugin).Return(mockDriverConn, nil).MinTimes(5)
	mockPluginService.EXPECT().GetHostRole(mockDriverConn).Return(host_info_util.READER).MinTimes(5)
	mockPluginService.EXPECT().ForceRefreshHostList(mockDriverConn).Return(nil).MinTimes(5)
	mockDriverConn.EXPECT().Close().MinTimes(5)

	actualConn, err2 := plugin.Connect(hostInfo, props, true, mockConnectFunc)

	assert.NoError(t, err0)
	assert.NoError(t, err1)
	// No writer could be verified, so the connect fails rather than returning the reader.
	assert.Error(t, err2)
	assert.Contains(t, err2.Error(), property_util.VERIFY_INITIAL_CONNECTION_TYPE.Name)
	assert.Nil(t, actualConn)
}

func TestAuroraInitialConnectionStrategyPlugin_Connect_WithRdsWriterCluster_AndLoginError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHostListProviderService := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)
	mockInitHostProviderFunc := func() error {
		return nil
	}
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()

	hostWriter1 := &host_info_util.HostInfo{Role: host_info_util.WRITER, Host: "writer1"}
	hostReader1 := &host_info_util.HostInfo{Role: host_info_util.READER, Host: "reader1"}
	hostReader2 := &host_info_util.HostInfo{Role: host_info_util.READER, Host: "reader2"}
	hosts := []*host_info_util.HostInfo{hostReader1, hostReader2, hostWriter1}
	mockPluginService.EXPECT().GetAllHosts().Return(hosts)

	props := MakeMapFromKeysAndVals(property_util.VERIFY_INITIAL_CONNECTION_TYPE.Name, "writer")
	hostInfo, err0 := host_info_util.NewHostInfoBuilder().
		SetHost("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com").
		Build()

	mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		return nil, nil
	}

	plugin, err1 := plugins.NewAuroraInitialConnectionStrategyPlugin(mockContainer, props)

	_ = plugin.InitHostProvider(props, mockHostListProviderService, mockInitHostProviderFunc)

	mockDriverConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockErr := errors.New("someLoginError")
	mockPluginService.EXPECT().Connect(hostWriter1, props, plugin).Return(mockDriverConn, mockErr)
	mockPluginService.EXPECT().IsLoginError(mockErr).Return(true)
	mockDriverConn.EXPECT().Close()

	actualConn, err2 := plugin.Connect(hostInfo, props, true, mockConnectFunc)

	assert.NoError(t, err0)
	assert.NoError(t, err1)
	assert.Error(t, err2)
	assert.Equal(t, nil, actualConn)
}

func TestAuroraInitialConnectionStrategyPlugin_Connect_WithRdsReaderCluster(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHostListProviderService := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)
	mockInitHostProviderFunc := func() error {
		return nil
	}
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()

	hostWriter1 := &host_info_util.HostInfo{Role: host_info_util.WRITER, Host: "writer1"}
	hostReader1 := &host_info_util.HostInfo{Role: host_info_util.READER, Host: "reader1"}
	hostReader2 := &host_info_util.HostInfo{Role: host_info_util.READER, Host: "reader2"}
	hosts := []*host_info_util.HostInfo{hostReader1, hostReader2, hostWriter1}
	mockPluginService.EXPECT().GetHosts().Return(hosts)
	mockHostListProviderService.EXPECT().SetInitialConnectionHostInfo(hostReader2)

	mockPluginService.EXPECT().AcceptsStrategy(gomock.Any()).Return(true)
	mockPluginService.EXPECT().GetHostInfoByStrategy(host_info_util.READER, gomock.Any(), gomock.Any()).Return(hostReader2, nil)

	props := MakeMapFromKeysAndVals(property_util.VERIFY_INITIAL_CONNECTION_TYPE.Name, "reader")
	hostInfo, err0 := host_info_util.NewHostInfoBuilder().
		SetHost("database-test-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com").
		Build()

	mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		return nil, nil
	}

	plugin, err1 := plugins.NewAuroraInitialConnectionStrategyPlugin(mockContainer, props)

	_ = plugin.InitHostProvider(props, mockHostListProviderService, mockInitHostProviderFunc)

	mockDriverConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockPluginService.EXPECT().Connect(hostReader2, props, plugin).Return(mockDriverConn, nil)
	mockPluginService.EXPECT().GetHostRole(mockDriverConn).Return(host_info_util.READER)

	actualConn, err2 := plugin.Connect(hostInfo, props, true, mockConnectFunc)

	assert.NoError(t, err0)
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.Equal(t, mockDriverConn, actualConn)
}

func TestAuroraInitialConnectionStrategyPlugin_Connect_WithRdsReaderCluster_AndNilReaderCandidateFallback(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHostListProviderService := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)
	mockInitHostProviderFunc := func() error {
		return nil
	}
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()

	hostWriter1 := &host_info_util.HostInfo{Role: host_info_util.WRITER, Host: "writer1"}
	hostReader1 := &host_info_util.HostInfo{Role: host_info_util.READER, Host: "reader1"}
	hostReader2 := &host_info_util.HostInfo{Role: host_info_util.READER, Host: "reader2"}
	hosts := []*host_info_util.HostInfo{hostReader1, hostReader2, hostWriter1}
	mockPluginService.EXPECT().GetHosts().Return(hosts)
	mockHostListProviderService.EXPECT().SetInitialConnectionHostInfo(hostReader2)

	mockPluginService.EXPECT().AcceptsStrategy(gomock.Any()).Return(true)
	mockPluginService.EXPECT().GetHostInfoByStrategy(host_info_util.READER, gomock.Any(), gomock.Any()).Return(nil, nil)

	props := MakeMapFromKeysAndVals(property_util.VERIFY_INITIAL_CONNECTION_TYPE.Name, "reader")
	hostInfo, err0 := host_info_util.NewHostInfoBuilder().
		SetHost("database-test-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com").
		Build()

	mockDriverConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		return mockDriverConn, nil
	}

	plugin, err1 := plugins.NewAuroraInitialConnectionStrategyPlugin(mockContainer, props)

	_ = plugin.InitHostProvider(props, mockHostListProviderService, mockInitHostProviderFunc)

	mockPluginService.EXPECT().ForceRefreshHostList(mockDriverConn).Return(nil)
	mockPluginService.EXPECT().IdentifyConnection(mockDriverConn).Return(hostReader2, nil)

	actualConn, err2 := plugin.Connect(hostInfo, props, true, mockConnectFunc)

	assert.NoError(t, err0)
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.Equal(t, mockDriverConn, actualConn)
}

func TestAuroraInitialConnectionStrategyPlugin_Connect_WithRdsReaderCluster_AndOutdatedTopology(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHostListProviderService := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)
	mockInitHostProviderFunc := func() error {
		return nil
	}
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()

	hostReader2 := &host_info_util.HostInfo{Role: host_info_util.READER, Host: "reader2"}
	hosts := []*host_info_util.HostInfo{}
	mockPluginService.EXPECT().GetHosts().Return(hosts)

	mockPluginService.EXPECT().AcceptsStrategy(gomock.Any()).Return(true)
	mockPluginService.EXPECT().GetHostInfoByStrategy(host_info_util.READER, gomock.Any(), gomock.Any()).Return(nil, nil)

	props := MakeMapFromKeysAndVals(property_util.VERIFY_INITIAL_CONNECTION_TYPE.Name, "reader")
	hostInfo, err0 := host_info_util.NewHostInfoBuilder().
		SetHost("database-test-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com").
		Build()

	mockDriverConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		return mockDriverConn, nil
	}

	mockPluginService.EXPECT().ForceRefreshHostList(mockDriverConn).Return(nil)
	mockPluginService.EXPECT().IdentifyConnection(mockDriverConn).Return(hostReader2, nil)
	mockHostListProviderService.EXPECT().SetInitialConnectionHostInfo(hostReader2)

	plugin, err1 := plugins.NewAuroraInitialConnectionStrategyPlugin(mockContainer, props)

	_ = plugin.InitHostProvider(props, mockHostListProviderService, mockInitHostProviderFunc)

	actualConn, err2 := plugin.Connect(hostInfo, props, true, mockConnectFunc)

	assert.NoError(t, err0)
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.Equal(t, mockDriverConn, actualConn)
}

func TestAuroraInitialConnectionStrategyPlugin_Connect_WithRdsReaderCluster_AndNoReaders(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHostListProviderService := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)
	mockInitHostProviderFunc := func() error {
		return nil
	}
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()

	hostWriter1 := &host_info_util.HostInfo{Role: host_info_util.WRITER, Host: "writer1"}
	hosts := []*host_info_util.HostInfo{hostWriter1}
	mockPluginService.EXPECT().GetHosts().Return(hosts)
	mockHostListProviderService.EXPECT().SetInitialConnectionHostInfo(hostWriter1)

	mockPluginService.EXPECT().AcceptsStrategy(gomock.Any()).Return(true)
	mockPluginService.EXPECT().GetHostInfoByStrategy(host_info_util.READER, gomock.Any(), gomock.Any()).Return(hostWriter1, nil)

	props := MakeMapFromKeysAndVals(property_util.VERIFY_INITIAL_CONNECTION_TYPE.Name, "reader")
	hostInfo, err0 := host_info_util.NewHostInfoBuilder().
		SetHost("database-test-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com").
		Build()

	mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		return nil, nil
	}

	plugin, err1 := plugins.NewAuroraInitialConnectionStrategyPlugin(mockContainer, props)

	_ = plugin.InitHostProvider(props, mockHostListProviderService, mockInitHostProviderFunc)

	mockDriverConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockPluginService.EXPECT().Connect(hostWriter1, props, plugin).Return(mockDriverConn, nil)
	mockPluginService.EXPECT().GetHostRole(mockDriverConn).Return(host_info_util.WRITER)
	mockPluginService.EXPECT().ForceRefreshHostList(mockDriverConn).Return(nil)
	mockPluginService.EXPECT().GetAllHosts().Return(hosts).Times(2)

	actualConn, err2 := plugin.Connect(hostInfo, props, true, mockConnectFunc)

	assert.NoError(t, err0)
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.Equal(t, mockDriverConn, actualConn)
}

func TestAuroraInitialConnectionStrategyPlugin_Connect_WithRdsReaderCluster_AndLoginError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHostListProviderService := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)
	mockInitHostProviderFunc := func() error {
		return nil
	}
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()

	hostWriter1 := &host_info_util.HostInfo{Role: host_info_util.WRITER, Host: "writer1"}
	hostReader1 := &host_info_util.HostInfo{Role: host_info_util.READER, Host: "reader1"}
	hostReader2 := &host_info_util.HostInfo{Role: host_info_util.READER, Host: "reader2"}
	hosts := []*host_info_util.HostInfo{hostReader1, hostReader2, hostWriter1}
	mockPluginService.EXPECT().GetHosts().Return(hosts)

	props := MakeMapFromKeysAndVals(property_util.VERIFY_INITIAL_CONNECTION_TYPE.Name, "reader")
	hostInfo, err0 := host_info_util.NewHostInfoBuilder().
		SetHost("database-test-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com").
		Build()

	mockConnectFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		return nil, nil
	}

	plugin, err1 := plugins.NewAuroraInitialConnectionStrategyPlugin(mockContainer, props)

	_ = plugin.InitHostProvider(props, mockHostListProviderService, mockInitHostProviderFunc)

	mockDriverConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockErr := errors.New("someLoginError")
	mockPluginService.EXPECT().AcceptsStrategy(gomock.Any()).Return(true)
	mockPluginService.EXPECT().GetHostInfoByStrategy(host_info_util.READER, gomock.Any(), gomock.Any()).Return(hostReader2, nil)
	mockPluginService.EXPECT().Connect(hostReader2, props, plugin).Return(mockDriverConn, mockErr)
	mockPluginService.EXPECT().IsLoginError(mockErr).Return(true)
	mockDriverConn.EXPECT().Close()

	actualConn, err2 := plugin.Connect(hostInfo, props, true, mockConnectFunc)

	assert.NoError(t, err0)
	assert.NoError(t, err1)
	assert.Error(t, err2)
	assert.Equal(t, nil, actualConn)
}
