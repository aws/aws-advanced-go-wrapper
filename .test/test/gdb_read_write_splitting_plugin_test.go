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

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	mock_database_sql_driver "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/database_sql_driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/plugins/read_write_splitting"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

// Hostnames that resolve to known regions via GetRdsRegion / GetRegionFromHost.
const (
	writerHostUSEast1  = "my-cluster.cluster-abc.us-east-1.rds.amazonaws.com"
	readerHostUSEast1  = "my-cluster.cluster-ro-abc.us-east-1.rds.amazonaws.com"
	reader2HostUSEast1 = "my-cluster2.cluster-ro-def.us-east-1.rds.amazonaws.com"
	writerHostEUWest1  = "my-cluster.cluster-abc.eu-west-1.rds.amazonaws.com"
	readerHostEUWest1  = "my-cluster.cluster-ro-abc.eu-west-1.rds.amazonaws.com"

	globalEndpointHost = "my-global-cluster.global-abc123.global.rds.amazonaws.com"
)

func TestNewGdbReadWriteSplittingPluginFactory(t *testing.T) {
	factory := read_write_splitting.NewGdbReadWriteSplittingPluginFactory()
	assert.NotNil(t, factory)
}

func TestGdbReadWriteSplittingPluginFactory_GetInstance(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	factory := read_write_splitting.NewGdbReadWriteSplittingPluginFactory()
	props := MakeMapFromKeysAndVals("test", "value")

	instance, err := factory.GetInstance(mockContainer, props)
	assert.NoError(t, err)
	assert.NotNil(t, instance)
	assert.Equal(t, driver_infrastructure.GDB_READ_WRITE_SPLITTING_PLUGIN_CODE, instance.GetPluginCode())
}

func TestGdbStrategy_OnConnect_RegionFromProps(t *testing.T) {
	props := MakeMapFromKeysAndVals(
		property_util.GDB_RW_HOME_REGION.Name, "us-east-1",
	)
	strategy := read_write_splitting.NewGdbReadWriteSplittingStrategy(props)

	hostInfo := &host_info_util.HostInfo{Host: "some-non-rds-host"}
	err := strategy.OnConnect(hostInfo, props)
	assert.NoError(t, err)
}

func TestGdbStrategy_OnConnect_RegionFromHost(t *testing.T) {
	props := MakeMapFromKeysAndVals()
	strategy := read_write_splitting.NewGdbReadWriteSplittingStrategy(props)

	hostInfo := &host_info_util.HostInfo{Host: writerHostUSEast1}
	err := strategy.OnConnect(hostInfo, props)
	assert.NoError(t, err)
}

func TestGdbStrategy_OnConnect_MissingRegion(t *testing.T) {
	props := MakeMapFromKeysAndVals()
	strategy := read_write_splitting.NewGdbReadWriteSplittingStrategy(props)

	hostInfo := &host_info_util.HostInfo{Host: "some-non-rds-host"}
	err := strategy.OnConnect(hostInfo, props)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "home region could not be determined")
}

func TestGdbStrategy_OnConnect_CalledOnce(t *testing.T) {
	props := MakeMapFromKeysAndVals(
		property_util.GDB_RW_HOME_REGION.Name, "us-east-1",
	)
	strategy := read_write_splitting.NewGdbReadWriteSplittingStrategy(props)

	hostInfo := &host_info_util.HostInfo{Host: writerHostUSEast1}
	err := strategy.OnConnect(hostInfo, props)
	assert.NoError(t, err)

	// Second call with a bad host should still succeed (isInit = true, skips).
	badHost := &host_info_util.HostInfo{Host: "bad-host"}
	err = strategy.OnConnect(badHost, props)
	assert.NoError(t, err)
}

func TestGdbStrategy_GetWriterConnection_HomeRegion(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	props := MakeMapFromKeysAndVals(
		property_util.GDB_RW_HOME_REGION.Name, "us-east-1",
		property_util.GDB_RW_RESTRICT_WRITER_TO_HOME_REGION.Name, "true",
	)
	strategy := read_write_splitting.NewGdbReadWriteSplittingStrategy(props)
	hostInfo := &host_info_util.HostInfo{Host: writerHostUSEast1}
	_ = strategy.OnConnect(hostInfo, props)

	writerHost := &host_info_util.HostInfo{Role: host_info_util.WRITER, Host: writerHostUSEast1}
	hosts := []*host_info_util.HostInfo{writerHost}

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()
	mockPluginService.EXPECT().Connect(writerHost, props, nil).Return(mockConn, nil)

	conn, host, err := strategy.GetWriterConnection(mockContainer, props, hosts, nil)
	assert.NoError(t, err)
	assert.Equal(t, mockConn, conn)
	assert.Equal(t, writerHost, host)
}

func TestGdbStrategy_GetWriterConnection_WriterNotInHomeRegion(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	props := MakeMapFromKeysAndVals(
		property_util.GDB_RW_HOME_REGION.Name, "us-east-1",
		property_util.GDB_RW_RESTRICT_WRITER_TO_HOME_REGION.Name, "true",
	)
	strategy := read_write_splitting.NewGdbReadWriteSplittingStrategy(props)
	hostInfo := &host_info_util.HostInfo{Host: writerHostUSEast1}
	_ = strategy.OnConnect(hostInfo, props)

	// Writer is in eu-west-1, but home region is us-east-1.
	writerHost := &host_info_util.HostInfo{Role: host_info_util.WRITER, Host: writerHostEUWest1}
	hosts := []*host_info_util.HostInfo{writerHost}

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()

	conn, host, err := strategy.GetWriterConnection(mockContainer, props, hosts, nil)
	assert.Error(t, err)
	assert.Nil(t, conn)
	assert.Nil(t, host)
	assert.Contains(t, err.Error(), "not in the home region")
}

func TestGdbStrategy_GetWriterConnection_RestrictionDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	props := MakeMapFromKeysAndVals(
		property_util.GDB_RW_HOME_REGION.Name, "us-east-1",
		property_util.GDB_RW_RESTRICT_WRITER_TO_HOME_REGION.Name, "false",
	)
	strategy := read_write_splitting.NewGdbReadWriteSplittingStrategy(props)
	hostInfo := &host_info_util.HostInfo{Host: writerHostUSEast1}
	_ = strategy.OnConnect(hostInfo, props)

	// Writer is in eu-west-1 but restriction is disabled — should succeed.
	writerHost := &host_info_util.HostInfo{Role: host_info_util.WRITER, Host: writerHostEUWest1}
	hosts := []*host_info_util.HostInfo{writerHost}

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()
	mockPluginService.EXPECT().Connect(writerHost, props, nil).Return(mockConn, nil)

	conn, host, err := strategy.GetWriterConnection(mockContainer, props, hosts, nil)
	assert.NoError(t, err)
	assert.Equal(t, mockConn, conn)
	assert.Equal(t, writerHost, host)
}

func TestGdbStrategy_GetWriterConnection_NoWriterInHosts(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	props := MakeMapFromKeysAndVals(
		property_util.GDB_RW_HOME_REGION.Name, "us-east-1",
	)
	strategy := read_write_splitting.NewGdbReadWriteSplittingStrategy(props)
	hostInfo := &host_info_util.HostInfo{Host: writerHostUSEast1}
	_ = strategy.OnConnect(hostInfo, props)

	// Only readers in the host list.
	hosts := []*host_info_util.HostInfo{
		{Role: host_info_util.READER, Host: readerHostUSEast1},
	}

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()

	conn, host, err := strategy.GetWriterConnection(mockContainer, props, hosts, nil)
	assert.Error(t, err)
	assert.Nil(t, conn)
	assert.Nil(t, host)
	assert.Contains(t, err.Error(), error_util.GetMessage("ReadWriteSplittingPlugin.noWriterFound"))
}

func TestGdbStrategy_GetReaderConnection_HomeRegionFiltering(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	props := MakeMapFromKeysAndVals(
		property_util.GDB_RW_HOME_REGION.Name, "us-east-1",
		property_util.GDB_RW_RESTRICT_READER_TO_HOME_REGION.Name, "true",
	)
	strategy := read_write_splitting.NewGdbReadWriteSplittingStrategy(props)
	hostInfo := &host_info_util.HostInfo{Host: writerHostUSEast1}
	_ = strategy.OnConnect(hostInfo, props)

	readerHome := &host_info_util.HostInfo{Role: host_info_util.READER, Host: readerHostUSEast1}
	readerForeign := &host_info_util.HostInfo{Role: host_info_util.READER, Host: readerHostEUWest1}
	writerHost := &host_info_util.HostInfo{Role: host_info_util.WRITER, Host: writerHostUSEast1}
	hosts := []*host_info_util.HostInfo{readerHome, readerForeign, writerHost}

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()

	// The strategy filters to home-region hosts, then delegates to DefaultStrategy
	// which calls GetHostInfoByStrategy on the filtered list.
	mockPluginService.EXPECT().GetHostInfoByStrategy(
		host_info_util.READER, gomock.Any(), gomock.Any(),
	).DoAndReturn(func(_ host_info_util.HostRole, _ string, filtered []*host_info_util.HostInfo) (*host_info_util.HostInfo, error) {
		// Verify the foreign reader was filtered out.
		for _, h := range filtered {
			assert.NotEqual(t, readerHostEUWest1, h.Host, "foreign reader should be filtered out")
		}
		return readerHome, nil
	})
	mockPluginService.EXPECT().Connect(readerHome, props, nil).Return(mockConn, nil)

	conn, host, err := strategy.GetReaderConnection(mockContainer, props, hosts, "random", nil)
	assert.NoError(t, err)
	assert.Equal(t, mockConn, conn)
	assert.Equal(t, readerHome, host)
}

func TestGdbStrategy_GetReaderConnection_NoReadersInHomeRegion(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	props := MakeMapFromKeysAndVals(
		property_util.GDB_RW_HOME_REGION.Name, "us-east-1",
		property_util.GDB_RW_RESTRICT_READER_TO_HOME_REGION.Name, "true",
	)
	strategy := read_write_splitting.NewGdbReadWriteSplittingStrategy(props)
	hostInfo := &host_info_util.HostInfo{Host: writerHostUSEast1}
	_ = strategy.OnConnect(hostInfo, props)

	// All hosts are in eu-west-1 — no hosts match the home region at all.
	readerForeign := &host_info_util.HostInfo{Role: host_info_util.READER, Host: readerHostEUWest1}
	writerForeign := &host_info_util.HostInfo{Role: host_info_util.WRITER, Host: writerHostEUWest1}
	hosts := []*host_info_util.HostInfo{readerForeign, writerForeign}

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()

	conn, host, err := strategy.GetReaderConnection(mockContainer, props, hosts, "random", nil)
	assert.Error(t, err)
	assert.Nil(t, conn)
	assert.Nil(t, host)
	assert.Contains(t, err.Error(), "No available reader hosts found in the home region")
}

func TestGdbStrategy_GetReaderConnection_RestrictionDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	props := MakeMapFromKeysAndVals(
		property_util.GDB_RW_HOME_REGION.Name, "us-east-1",
		property_util.GDB_RW_RESTRICT_READER_TO_HOME_REGION.Name, "false",
	)
	strategy := read_write_splitting.NewGdbReadWriteSplittingStrategy(props)
	hostInfo := &host_info_util.HostInfo{Host: writerHostUSEast1}
	_ = strategy.OnConnect(hostInfo, props)

	readerForeign := &host_info_util.HostInfo{Role: host_info_util.READER, Host: readerHostEUWest1}
	writerHost := &host_info_util.HostInfo{Role: host_info_util.WRITER, Host: writerHostUSEast1}
	hosts := []*host_info_util.HostInfo{readerForeign, writerHost}

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()

	// With restriction disabled, all hosts are passed through — including the foreign reader.
	mockPluginService.EXPECT().GetHostInfoByStrategy(
		host_info_util.READER, gomock.Any(), gomock.Any(),
	).Return(readerForeign, nil)
	mockPluginService.EXPECT().Connect(readerForeign, props, nil).Return(mockConn, nil)

	conn, host, err := strategy.GetReaderConnection(mockContainer, props, hosts, "random", nil)
	assert.NoError(t, err)
	assert.Equal(t, mockConn, conn)
	assert.Equal(t, readerForeign, host)
}

func TestGdbPlugin_Connect_InitializesHomeRegion(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	props := MakeMapFromKeysAndVals(
		property_util.GDB_RW_HOME_REGION.Name, "us-east-1",
	)

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()
	mockHostProvider := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)
	mockPluginService.EXPECT().AcceptsStrategy(gomock.Any()).Return(true)
	mockHostProvider.EXPECT().IsStaticHostListProvider().Return(true)

	strategy := read_write_splitting.NewGdbReadWriteSplittingStrategy(props)
	plugin := read_write_splitting.NewReadWriteSplittingPlugin(
		mockContainer, props,
		driver_infrastructure.GDB_READ_WRITE_SPLITTING_PLUGIN_CODE,
		strategy)
	_ = plugin.InitHostProvider(nil, mockHostProvider, func() error { return nil })

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	hostInfo := &host_info_util.HostInfo{Host: writerHostUSEast1}

	conn, err := plugin.Connect(hostInfo, props, true, func(_ *utils.RWMap[string, string]) (driver.Conn, error) {
		return mockConn, nil
	})
	assert.NoError(t, err)
	assert.Equal(t, mockConn, conn)
}

func TestGdbPlugin_Connect_MissingHomeRegion(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	props := MakeMapFromKeysAndVals()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()

	strategy := read_write_splitting.NewGdbReadWriteSplittingStrategy(props)
	plugin := read_write_splitting.NewReadWriteSplittingPlugin(
		mockContainer, props,
		driver_infrastructure.GDB_READ_WRITE_SPLITTING_PLUGIN_CODE,
		strategy)

	// Non-RDS host and no property — region can't be determined.
	hostInfo := &host_info_util.HostInfo{Host: "localhost"}

	conn, err := plugin.Connect(hostInfo, props, true, func(_ *utils.RWMap[string, string]) (driver.Conn, error) {
		return nil, nil
	})
	assert.Error(t, err)
	assert.Nil(t, conn)
	assert.Contains(t, err.Error(), "home region could not be determined")
}

func TestGdbStrategy_OnConnect_GlobalEndpoint_WithRegionProp(t *testing.T) {
	props := MakeMapFromKeysAndVals(
		property_util.GDB_RW_HOME_REGION.Name, "us-east-1",
	)
	strategy := read_write_splitting.NewGdbReadWriteSplittingStrategy(props)

	hostInfo := &host_info_util.HostInfo{Host: globalEndpointHost}
	err := strategy.OnConnect(hostInfo, props)
	assert.NoError(t, err)
}

func TestGdbStrategy_OnConnect_GlobalEndpoint_WithoutRegionProp(t *testing.T) {
	props := MakeMapFromKeysAndVals()
	strategy := read_write_splitting.NewGdbReadWriteSplittingStrategy(props)

	hostInfo := &host_info_util.HostInfo{Host: globalEndpointHost}
	err := strategy.OnConnect(hostInfo, props)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "home region could not be determined")
}

func TestGdbPlugin_Connect_GlobalEndpoint_WithRegionProp(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	props := MakeMapFromKeysAndVals(
		property_util.GDB_RW_HOME_REGION.Name, "us-east-1",
	)

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()
	mockHostProvider := mock_driver_infrastructure.NewMockHostListProviderService(ctrl)
	mockPluginService.EXPECT().AcceptsStrategy(gomock.Any()).Return(true)
	mockHostProvider.EXPECT().IsStaticHostListProvider().Return(true)

	strategy := read_write_splitting.NewGdbReadWriteSplittingStrategy(props)
	plugin := read_write_splitting.NewReadWriteSplittingPlugin(
		mockContainer, props,
		driver_infrastructure.GDB_READ_WRITE_SPLITTING_PLUGIN_CODE,
		strategy)
	_ = plugin.InitHostProvider(nil, mockHostProvider, func() error { return nil })

	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	hostInfo := &host_info_util.HostInfo{Host: globalEndpointHost}

	conn, err := plugin.Connect(hostInfo, props, true, func(_ *utils.RWMap[string, string]) (driver.Conn, error) {
		return mockConn, nil
	})
	assert.NoError(t, err)
	assert.Equal(t, mockConn, conn)
}

func TestGdbPlugin_Connect_GlobalEndpoint_MissingRegionProp(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	props := MakeMapFromKeysAndVals()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()

	strategy := read_write_splitting.NewGdbReadWriteSplittingStrategy(props)
	plugin := read_write_splitting.NewReadWriteSplittingPlugin(
		mockContainer, props,
		driver_infrastructure.GDB_READ_WRITE_SPLITTING_PLUGIN_CODE,
		strategy)

	hostInfo := &host_info_util.HostInfo{Host: globalEndpointHost}

	conn, err := plugin.Connect(hostInfo, props, true, func(_ *utils.RWMap[string, string]) (driver.Conn, error) {
		return nil, nil
	})
	assert.Error(t, err)
	assert.Nil(t, conn)
	assert.Contains(t, err.Error(), "home region could not be determined")
}

func TestGdbPlugin_Execute_SwitchToReaderFiltered(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	props := MakeMapFromKeysAndVals(
		property_util.GDB_RW_HOME_REGION.Name, "us-east-1",
		property_util.GDB_RW_RESTRICT_READER_TO_HOME_REGION.Name, "true",
	)

	writerHost := &host_info_util.HostInfo{Role: host_info_util.WRITER, Host: writerHostUSEast1}
	readerHome := &host_info_util.HostInfo{Role: host_info_util.READER, Host: readerHostUSEast1}
	readerForeign := &host_info_util.HostInfo{Role: host_info_util.READER, Host: readerHostEUWest1}
	hosts := []*host_info_util.HostInfo{readerHome, readerForeign, writerHost}

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()

	strategy := read_write_splitting.NewGdbReadWriteSplittingStrategy(props)
	// Initialize the strategy's home region.
	_ = strategy.OnConnect(&host_info_util.HostInfo{Host: writerHostUSEast1}, props)

	plugin := read_write_splitting.NewReadWriteSplittingPlugin(
		mockContainer, props,
		driver_infrastructure.GDB_READ_WRITE_SPLITTING_PLUGIN_CODE,
		strategy)

	mockWriterConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockReaderConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockDriverDialect := mock_driver_infrastructure.NewMockDriverDialect(ctrl)

	mockPluginService.EXPECT().GetTargetDriverDialect().Return(mockDriverDialect).AnyTimes()
	mockDriverDialect.EXPECT().IsClosed(gomock.Any()).Return(false).AnyTimes()
	mockPluginService.EXPECT().RefreshHostList(gomock.Any()).Return(nil).AnyTimes()
	mockPluginService.EXPECT().IsInTransaction().Return(false).AnyTimes()
	mockPluginService.EXPECT().GetHosts().Return(hosts).AnyTimes()

	mockPluginService.EXPECT().GetCurrentConnection().Return(mockWriterConn).Times(3)
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(writerHost, nil).Times(2)

	// The GDB strategy filters to home-region readers, then the default strategy picks one.
	mockPluginService.EXPECT().GetHostInfoByStrategy(
		host_info_util.READER, gomock.Any(), gomock.Any(),
	).DoAndReturn(func(_ host_info_util.HostRole, _ string, filtered []*host_info_util.HostInfo) (*host_info_util.HostInfo, error) {
		for _, h := range filtered {
			assert.NotEqual(t, readerHostEUWest1, h.Host)
		}
		return readerHome, nil
	})
	mockPluginService.EXPECT().Connect(readerHome, props, plugin).Return(mockReaderConn, nil)
	mockPluginService.EXPECT().SetCurrentConnection(mockReaderConn, readerHome, nil).Return(nil)

	executeFunc := func() (any, any, bool, error) { return nil, nil, false, nil }
	val1, val2, ok, err := plugin.Execute(nil, plugin_helpers.SET_READ_ONLY_METHOD, executeFunc, true)

	assert.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, val1)
	assert.Nil(t, val2)
}
