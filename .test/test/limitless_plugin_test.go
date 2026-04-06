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
	"time"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/plugins/limitless"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/services"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils/telemetry"
	mysql_driver "github.com/aws/aws-advanced-go-wrapper/mysql-driver"
	pgx_driver "github.com/aws/aws-advanced-go-wrapper/pgx-driver"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

var pgLimitlessTestDsn = "user=someUser password=somePassword host=mydb-1-db-shard-group-1.shardgrp-xyz.us-east-2.rds.amazonaws.com port=5432 database=postgres_limitless " +
	"plugins=limitless"

func beforeLimitlessPluginConnectTest(t *testing.T, props *utils.RWMap[string, string]) (*plugin_helpers.PluginServiceImpl, *services.FullServicesContainer) {
	ctrl := gomock.NewController(t)
	mockTargetDriver := &MockTargetDriver{}
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)

	storage := services.NewExpiringStorage(5*time.Minute, nil)
	driver_infrastructure.RegisterDefaultStorageTypes(storage)

	mockMonitorService := mock_driver_infrastructure.NewMockMonitorService(ctrl)
	mockMonitorService.EXPECT().RegisterMonitorType(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	mockMonitorService.EXPECT().RunIfAbsent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	mockMonitorService.EXPECT().StopAndRemove(gomock.Any(), gomock.Any()).AnyTimes()

	container := &services.FullServicesContainer{
		Storage:   storage,
		Monitor:   mockMonitorService,
		Telemetry: telemetryFactory,
	}

	mockPluginManager := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, container, props)
	container.PluginManager = mockPluginManager

	pluginService, _ := plugin_helpers.NewPluginServiceImpl(container, pgx_driver.NewPgxDriverDialect(), props, pgLimitlessTestDsn)
	pluginServiceImpl, _ := pluginService.(*plugin_helpers.PluginServiceImpl)
	pluginServiceImpl.SetDialect(&driver_infrastructure.AuroraPgDatabaseDialect{})
	container.PluginService = pluginService

	return pluginServiceImpl, container
}

func TestCreateLimitlessPluginWithNonShardGroupUrlValidateTrue(t *testing.T) {
	host := "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com"
	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.HOST.Name, host,
	)
	_, container := beforeLimitlessPluginConnectTest(t, props)
	pluginFactory := limitless.LimitlessPluginFactory{}
	plugin, err := pluginFactory.GetInstance(container, props)
	assert.Error(t, err)
	assert.Nil(t, plugin)
	assert.Equal(t, err, error_util.NewGenericAwsWrapperError(error_util.GetMessage("LimitlessPlugin.expectedShardGroupUrl", host)))
}

func TestCreateLimitlessPluginWithNonShardGroupUrlValidateFalse(t *testing.T) {
	host := "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com"
	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.HOST.Name, host,
		property_util.LIMITLESS_USE_SHARD_GROUP_URL.Name, "false",
	)
	_, container := beforeLimitlessPluginConnectTest(t, props)
	pluginFactory := limitless.LimitlessPluginFactory{}
	plugin, err := pluginFactory.GetInstance(container, props)
	assert.Nil(t, err)
	assert.NotNil(t, plugin)
}

func TestCreateLimitlessPluginWithShardGroupUrlValidateTrue(t *testing.T) {
	host := "mydb-1-db-shard-group-1.shardgrp-xyz.us-east-2.rds.amazonaws.com"
	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.HOST.Name, host,
	)
	_, container := beforeLimitlessPluginConnectTest(t, props)
	pluginFactory := limitless.LimitlessPluginFactory{}
	plugin, err := pluginFactory.GetInstance(container, props)
	assert.Nil(t, err)
	assert.NotNil(t, plugin)
}

func TestCreateLimitlessPluginWithShardGroupUrlValidateFalse(t *testing.T) {
	host := "mydb-1-db-shard-group-1.shardgrp-xyz.us-east-2.rds.amazonaws.com"
	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.HOST.Name, host,
		property_util.LIMITLESS_USE_SHARD_GROUP_URL.Name, "false",
	)
	_, container := beforeLimitlessPluginConnectTest(t, props)
	pluginFactory := limitless.LimitlessPluginFactory{}
	plugin, err := pluginFactory.GetInstance(container, props)
	assert.Nil(t, err)
	assert.NotNil(t, plugin)
}

func TestLimitlessPluginConnect(t *testing.T) {
	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
	)

	_, container := beforeLimitlessPluginConnectTest(t, props)

	mockConn := &MockConn{}
	mockLimitlessRouterService := &MockLimitlessRouterService{}
	mockLimitlessRouterService.SetEstablishConnectionFunc(func(context *limitless.LimitlessConnectionContext) error {
		context.SetConnection(mockConn)
		return nil
	})

	someHostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("test").SetPort(1234).SetRole(host_info_util.WRITER).Build()

	limitlessPlugin := limitless.NewLimitlessPluginWithRouterService(
		container,
		emptyProps,
		mockLimitlessRouterService)

	actualConn, err := limitlessPlugin.Connect(
		someHostInfo,
		props,
		true,
		nil)

	assert.Nil(t, err)
	assert.Equal(t, mockConn, actualConn)
	assert.Equal(t, 1, mockLimitlessRouterService.startMonitoringCallCount)
	assert.Equal(t, 1, mockLimitlessRouterService.establishConnectionCallCount)
}

func TestLimitlessPluginConnectGivenDialectRecoverySuccess(t *testing.T) {
	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
	)
	pluginServiceImpl, container := beforeLimitlessPluginConnectTest(t, props)
	pluginServiceImpl.SetDialect(&driver_infrastructure.PgDatabaseDialect{})

	mockConn := &MockConn{}
	mockLimitlessRouterService := &MockLimitlessRouterService{}

	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		pluginServiceImpl.SetDialect(&driver_infrastructure.AuroraPgDatabaseDialect{})
		return mockConn, nil
	}

	someHostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("test").SetPort(1234).SetRole(host_info_util.WRITER).Build()

	limitlessPlugin := limitless.NewLimitlessPluginWithRouterService(
		container,
		emptyProps,
		mockLimitlessRouterService)

	actualConn, err := limitlessPlugin.Connect(
		someHostInfo,
		props,
		true,
		mockConnFunc)

	assert.Nil(t, err)
	assert.Equal(t, mockConn, actualConn)
	assert.Equal(t, 1, mockLimitlessRouterService.startMonitoringCallCount)
	assert.Equal(t, 1, mockLimitlessRouterService.establishConnectionCallCount)
}

func TestLimitlessPluginConnectGivenDialectRecoveryFailure(t *testing.T) {
	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
	)
	pluginServiceImpl, container := beforeLimitlessPluginConnectTest(t, props)
	pluginServiceImpl.SetDialect(&driver_infrastructure.PgDatabaseDialect{})

	mockConn := &MockConn{}
	mockLimitlessRouterService := &MockLimitlessRouterService{}

	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		return mockConn, nil
	}

	someHostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("test").SetPort(1234).SetRole(host_info_util.WRITER).Build()

	limitlessPlugin := limitless.NewLimitlessPluginWithRouterService(
		container,
		emptyProps,
		mockLimitlessRouterService)

	actualConn, err := limitlessPlugin.Connect(
		someHostInfo,
		props,
		true,
		mockConnFunc)

	assert.NotNil(t, err)
	assert.Nil(t, actualConn)
	assert.Equal(t, 0, mockLimitlessRouterService.startMonitoringCallCount)
	assert.Equal(t, 0, mockLimitlessRouterService.establishConnectionCallCount)
}

func TestLimitlessPluginConnectGivenContextMissingConn(t *testing.T) {
	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
	)

	_, container := beforeLimitlessPluginConnectTest(t, props)

	mockLimitlessRouterService := &MockLimitlessRouterService{}

	someHostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("test").SetPort(1234).SetRole(host_info_util.WRITER).Build()

	limitlessPlugin := limitless.NewLimitlessPluginWithRouterService(
		container,
		emptyProps,
		mockLimitlessRouterService)

	actualConn, err := limitlessPlugin.Connect(
		someHostInfo,
		props,
		true,
		nil)

	assert.NotNil(t, err)
	assert.Nil(t, actualConn)
	assert.Equal(t, 1, mockLimitlessRouterService.startMonitoringCallCount)
	assert.Equal(t, 1, mockLimitlessRouterService.establishConnectionCallCount)
}

func TestLimitlessPluginConnectGivenStartMonitoringThrows(t *testing.T) {
	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
	)

	_, container := beforeLimitlessPluginConnectTest(t, props)

	startMonitoringErrorString := "StartMonitoringError"
	mockLimitlessRouterService := &MockLimitlessRouterService{}
	mockLimitlessRouterService.SetStartMonitoringFunc(func(hostInfo *host_info_util.HostInfo, props *utils.RWMap[string, string], intervalMs int) error {
		return errors.New(startMonitoringErrorString)
	})

	someHostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("test").SetPort(1234).SetRole(host_info_util.WRITER).Build()

	limitlessPlugin := limitless.NewLimitlessPluginWithRouterService(
		container,
		emptyProps,
		mockLimitlessRouterService)

	actualConn, err := limitlessPlugin.Connect(
		someHostInfo,
		props,
		true,
		nil)

	assert.NotNil(t, err)
	assert.Equal(t, startMonitoringErrorString, err.Error())
	assert.Nil(t, actualConn)
	assert.Equal(t, 1, mockLimitlessRouterService.startMonitoringCallCount)
	assert.Equal(t, 0, mockLimitlessRouterService.establishConnectionCallCount)
}

func TestLimitlessPluginConnectGivenEstablishConnectionThrows(t *testing.T) {
	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
	)

	_, container := beforeLimitlessPluginConnectTest(t, props)

	establishConnectionErrorString := "EstablishConnectionError"
	mockLimitlessRouterService := &MockLimitlessRouterService{}
	mockLimitlessRouterService.SetEstablishConnectionFunc(func(context *limitless.LimitlessConnectionContext) error {
		return errors.New(establishConnectionErrorString)
	})

	someHostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("test").SetPort(1234).SetRole(host_info_util.WRITER).Build()

	limitlessPlugin := limitless.NewLimitlessPluginWithRouterService(
		container,
		emptyProps,
		mockLimitlessRouterService)

	actualConn, err := limitlessPlugin.Connect(
		someHostInfo,
		props,
		true,
		nil)

	assert.NotNil(t, err)
	assert.Equal(t, establishConnectionErrorString, err.Error())
	assert.Nil(t, actualConn)
	assert.Equal(t, 1, mockLimitlessRouterService.startMonitoringCallCount)
	assert.Equal(t, 1, mockLimitlessRouterService.establishConnectionCallCount)
}

type establishConnectionFuncType func(context *limitless.LimitlessConnectionContext) error
type startMonitoringFuncType func(hostInfo *host_info_util.HostInfo, props *utils.RWMap[string, string], intervalMs int) error
type MockLimitlessRouterService struct {
	establishConnectionFunc      establishConnectionFuncType
	startMonitoringFunc          startMonitoringFuncType
	establishConnectionCallCount int
	startMonitoringCallCount     int
}

func (m *MockLimitlessRouterService) SetEstablishConnectionFunc(fn establishConnectionFuncType) {
	m.establishConnectionFunc = fn
}

func (m *MockLimitlessRouterService) EstablishConnection(context *limitless.LimitlessConnectionContext) error {
	m.establishConnectionCallCount++
	if m.establishConnectionFunc == nil {
		return nil
	}
	return m.establishConnectionFunc(context)
}

func (m *MockLimitlessRouterService) SetStartMonitoringFunc(fn startMonitoringFuncType) {
	m.startMonitoringFunc = fn
}

func (m *MockLimitlessRouterService) StartMonitoring(hostInfo *host_info_util.HostInfo, props *utils.RWMap[string, string], intervalMs int) error {
	m.startMonitoringCallCount++
	if m.startMonitoringFunc == nil {
		return nil
	}
	return m.startMonitoringFunc(hostInfo, props, intervalMs)
}

func beforeLimitlessRouterServiceTest(
	t *testing.T, props *utils.RWMap[string, string], clusterId string,
) (*plugin_helpers.PluginServiceImpl, *services.FullServicesContainer) {
	ctrl := gomock.NewController(t)
	mockTargetDriver := &MockTargetDriver{}
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)

	storage := services.NewExpiringStorage(5*time.Minute, nil)
	driver_infrastructure.RegisterDefaultStorageTypes(storage)

	mockMonitorService := mock_driver_infrastructure.NewMockMonitorService(ctrl)
	mockMonitorService.EXPECT().RegisterMonitorType(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	mockMonitorService.EXPECT().RunIfAbsent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	mockMonitorService.EXPECT().StopAndRemove(gomock.Any(), gomock.Any()).AnyTimes()

	container := &services.FullServicesContainer{
		Storage:   storage,
		Monitor:   mockMonitorService,
		Telemetry: telemetryFactory,
	}

	mockPluginManager := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, container, props)
	container.PluginManager = mockPluginManager

	pluginService, _ := plugin_helpers.NewPluginServiceImpl(container, mysql_driver.NewMySQLDriverDialect(), props, pgLimitlessTestDsn)
	pluginServiceImpl, _ := pluginService.(*plugin_helpers.PluginServiceImpl)

	mockHostListProvider := &MockHostListProvider{}
	mockHostListProvider.SetClusterId(clusterId)
	pluginServiceImpl.SetHostListProvider(mockHostListProvider)

	container.PluginService = pluginService

	return pluginServiceImpl, container
}

func TestLimitlessMonitorServiceEstablishConnection(t *testing.T) {
	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
	)

	clusterId := "someClusterId"
	_, container := beforeLimitlessRouterServiceTest(t, props, clusterId)

	limitlessRouterService := limitless.NewLimitlessRouterServiceImplInternal(container, nil, props)

	cachedRouter0, _ := host_info_util.NewHostInfoBuilder().SetHost("host0").SetWeight(10).Build()
	cachedRouter1, _ := host_info_util.NewHostInfoBuilder().SetHost("host1").SetWeight(9).Build()
	cachedRouter2, _ := host_info_util.NewHostInfoBuilder().SetHost("host2").SetWeight(8).Build()
	cachedRouterList := []*host_info_util.HostInfo{cachedRouter0, cachedRouter1, cachedRouter2}
	limitless.LimitlessRoutersStorageType.Set(container.GetStorageService(), clusterId, limitless.NewLimitlessRouters(cachedRouterList))

	host, _ := host_info_util.NewHostInfoBuilder().SetHost("host1").SetWeight(9).Build()
	mockConn := &MockConn{}
	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		return mockConn, nil
	}
	context := limitless.NewConnectionContext(*host, props, nil, mockConnFunc, nil, nil)

	err := limitlessRouterService.EstablishConnection(context)
	actualConn := context.GetConnection()

	assert.Nil(t, err)
	assert.Equal(t, mockConn, actualConn)
}

func TestLimitlessMonitorServiceEstablishConnect_GivenEmptyCacheAndNoWaitForRouterInfo(t *testing.T) {
	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.LIMITLESS_WAIT_FOR_ROUTER_INFO.Name, "false",
	)

	clusterId := "someClusterId"
	_, container := beforeLimitlessRouterServiceTest(t, props, clusterId)

	limitlessRouterService := limitless.NewLimitlessRouterServiceImplInternal(container, nil, props)

	host, _ := host_info_util.NewHostInfoBuilder().SetHost("host1").SetWeight(9).Build()
	mockConn := &MockConn{}
	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		return mockConn, nil
	}
	context := limitless.NewConnectionContext(*host, props, nil, mockConnFunc, nil, nil)

	err := limitlessRouterService.EstablishConnection(context)
	actualConn := context.GetConnection()

	assert.Nil(t, err)
	assert.Equal(t, mockConn, actualConn)
}

func TestLimitlessMonitorServiceEstablishConnect_MaxRetries(t *testing.T) {
	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.LIMITLESS_WAIT_FOR_ROUTER_INFO.Name, "true",
	)

	clusterId := "someClusterId"
	_, container := beforeLimitlessRouterServiceTest(t, props, clusterId)

	mockLimitlessQueryHelper := &MockLimitlessQueryHelper{}

	limitlessRouterService := limitless.NewLimitlessRouterServiceImplInternal(container, mockLimitlessQueryHelper, props)

	host, _ := host_info_util.NewHostInfoBuilder().SetHost("host1").SetWeight(9).Build()
	mockConn := &MockConn{}
	mockConnFunc := func(props *utils.RWMap[string, string]) (driver.Conn, error) {
		return mockConn, nil
	}
	context := limitless.NewConnectionContext(*host, props, nil, mockConnFunc, nil, nil)

	err := limitlessRouterService.EstablishConnection(context)

	assert.NotNil(t, err)

	maxRetries := property_util.GetVerifiedWrapperPropertyValue[int](context.Props, property_util.LIMITLESS_GET_ROUTER_MAX_RETRIES)
	assert.Equal(t, maxRetries+1, mockLimitlessQueryHelper.queryForLimitlessRoutersCallCount)
}

type MockLimitlessQueryHelper struct {
	queryForLimitlessRoutersFunc      QueryForLimitlessRoutersFuncType
	queryForLimitlessRoutersCallCount int
}

type QueryForLimitlessRoutersFuncType func() (hostInfoList []*host_info_util.HostInfo, err error)

func (queryHelper *MockLimitlessQueryHelper) QueryForLimitlessRouters(
	_ driver.Conn,
	_ int,
	_ *utils.RWMap[string, string]) (hostInfoList []*host_info_util.HostInfo, err error) {
	queryHelper.queryForLimitlessRoutersCallCount++
	if queryHelper.queryForLimitlessRoutersFunc == nil {
		return nil, errors.New("someError")
	}
	return queryHelper.queryForLimitlessRoutersFunc()
}

func (queryHelper *MockLimitlessQueryHelper) SetQueryForLimitlessRoutersFunc(fn QueryForLimitlessRoutersFuncType) {
	queryHelper.queryForLimitlessRoutersFunc = fn
}
