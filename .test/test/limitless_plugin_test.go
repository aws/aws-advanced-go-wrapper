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
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/limitless"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	"github.com/aws/aws-advanced-go-wrapper/mysql-driver"
	"github.com/aws/aws-advanced-go-wrapper/pgx-driver"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var pgLimitlessTestDsn = "user=someUser password=somePassword host=mydb-1-db-shard-group-1.shardgrp-xyz.us-east-2.rds.amazonaws.com port=5432 database=postgres_limitless " +
	"plugins=limitless"

func beforeLimitlessPluginConnectTest(props map[string]string) *plugin_helpers.PluginServiceImpl {
	mockTargetDriver := &MockTargetDriver{}
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	mockPluginManager := plugin_helpers.NewPluginManagerImpl(
		mockTargetDriver,
		props, driver_infrastructure.ConnectionProviderManager{},
		telemetryFactory)
	pluginService, _ := plugin_helpers.NewPluginServiceImpl(mockPluginManager, pgx_driver.NewPgxDriverDialect(), props, pgLimitlessTestDsn)
	pluginServiceImpl, _ := pluginService.(*plugin_helpers.PluginServiceImpl)
	pluginServiceImpl.SetDialect(&driver_infrastructure.AuroraPgDatabaseDialect{})

	return pluginServiceImpl
}

func TestLimitlessPluginConnect(t *testing.T) {
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
	}

	pluginServiceImpl := beforeLimitlessPluginConnectTest(props)

	mockConn := &MockConn{}
	mockLimitlessRouterService := &MockLimitlessRouterService{}
	mockLimitlessRouterService.SetEstablishConnectionFunc(func(context *limitless.LimitlessConnectionContext) error {
		context.SetConnection(mockConn)
		return nil
	})

	someHostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("test").SetPort(1234).SetRole(host_info_util.WRITER).Build()

	limitlessPlugin := limitless.NewLimitlessPluginWithRouterService(
		driver_infrastructure.PluginService(pluginServiceImpl),
		map[string]string{},
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
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
	}
	pluginServiceImpl := beforeLimitlessPluginConnectTest(props)
	pluginServiceImpl.SetDialect(&driver_infrastructure.PgDatabaseDialect{})

	mockConn := &MockConn{}
	mockLimitlessRouterService := &MockLimitlessRouterService{}

	mockConnFunc := func(props map[string]string) (driver.Conn, error) {
		pluginServiceImpl.SetDialect(&driver_infrastructure.AuroraPgDatabaseDialect{})
		return mockConn, nil
	}

	someHostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("test").SetPort(1234).SetRole(host_info_util.WRITER).Build()

	limitlessPlugin := limitless.NewLimitlessPluginWithRouterService(
		driver_infrastructure.PluginService(pluginServiceImpl),
		map[string]string{},
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
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
	}
	pluginServiceImpl := beforeLimitlessPluginConnectTest(props)
	pluginServiceImpl.SetDialect(&driver_infrastructure.PgDatabaseDialect{})

	mockConn := &MockConn{}
	mockLimitlessRouterService := &MockLimitlessRouterService{}

	mockConnFunc := func(props map[string]string) (driver.Conn, error) {
		return mockConn, nil
	}

	someHostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("test").SetPort(1234).SetRole(host_info_util.WRITER).Build()

	limitlessPlugin := limitless.NewLimitlessPluginWithRouterService(
		driver_infrastructure.PluginService(pluginServiceImpl),
		map[string]string{},
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
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
	}

	pluginServiceImpl := beforeLimitlessPluginConnectTest(props)

	mockLimitlessRouterService := &MockLimitlessRouterService{}

	someHostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("test").SetPort(1234).SetRole(host_info_util.WRITER).Build()

	limitlessPlugin := limitless.NewLimitlessPluginWithRouterService(
		driver_infrastructure.PluginService(pluginServiceImpl),
		map[string]string{},
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
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
	}

	pluginServiceImpl := beforeLimitlessPluginConnectTest(props)

	startMonitoringErrorString := "StartMonitoringError"
	mockLimitlessRouterService := &MockLimitlessRouterService{}
	mockLimitlessRouterService.SetStartMonitoringFunc(func(hostInfo *host_info_util.HostInfo, props map[string]string, intervalMs int) error {
		return errors.New(startMonitoringErrorString)
	})

	someHostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("test").SetPort(1234).SetRole(host_info_util.WRITER).Build()

	limitlessPlugin := limitless.NewLimitlessPluginWithRouterService(
		driver_infrastructure.PluginService(pluginServiceImpl),
		map[string]string{},
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
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
	}

	pluginServiceImpl := beforeLimitlessPluginConnectTest(props)

	establishConnectionErrorString := "EstablishConnectionError"
	mockLimitlessRouterService := &MockLimitlessRouterService{}
	mockLimitlessRouterService.SetEstablishConnectionFunc(func(context *limitless.LimitlessConnectionContext) error {
		return errors.New(establishConnectionErrorString)
	})

	someHostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("test").SetPort(1234).SetRole(host_info_util.WRITER).Build()

	limitlessPlugin := limitless.NewLimitlessPluginWithRouterService(
		driver_infrastructure.PluginService(pluginServiceImpl),
		map[string]string{},
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
type startMonitoringFuncType func(hostInfo *host_info_util.HostInfo, props map[string]string, intervalMs int) error
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

func (m *MockLimitlessRouterService) StartMonitoring(hostInfo *host_info_util.HostInfo, props map[string]string, intervalMs int) error {
	m.startMonitoringCallCount++
	if m.startMonitoringFunc == nil {
		return nil
	}
	return m.startMonitoringFunc(hostInfo, props, intervalMs)
}

func TestLimitlessMonitorServiceEstablishConnection(t *testing.T) {
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name: "postgresql",
	}

	clusterId := "someClusterId"

	mockHostListProviderService := &MockHostListProvider{}
	mockHostListProviderService.SetClusterId(clusterId)

	mockTargetDriver := &MockTargetDriver{}
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	mockPluginManager := driver_infrastructure.PluginManager(
		plugin_helpers.NewPluginManagerImpl(
			mockTargetDriver,
			props, driver_infrastructure.ConnectionProviderManager{},
			telemetryFactory))
	pluginServiceImpl, _ := plugin_helpers.NewPluginServiceImpl(mockPluginManager, mysql_driver.NewMySQLDriverDialect(), props, pgLimitlessTestDsn)
	pluginServiceImpl.SetHostListProvider(mockHostListProviderService)

	limitlessRouterService := limitless.NewLimitlessRouterServiceImplInternal(pluginServiceImpl, nil, props)

	cachedRouter0, _ := host_info_util.NewHostInfoBuilder().SetHost("host0").SetWeight(10).Build()
	cachedRouter1, _ := host_info_util.NewHostInfoBuilder().SetHost("host1").SetWeight(9).Build()
	cachedRouter2, _ := host_info_util.NewHostInfoBuilder().SetHost("host2").SetWeight(8).Build()
	cachedRouterList := []*host_info_util.HostInfo{cachedRouter0, cachedRouter1, cachedRouter2}
	limitless.LIMITLESS_ROUTER_CACHE.Put(clusterId, cachedRouterList, time.Duration(1)*time.Minute)
	defer limitless.LIMITLESS_ROUTER_CACHE.Clear()

	host, _ := host_info_util.NewHostInfoBuilder().SetHost("host1").SetWeight(9).Build()
	mockConn := &MockConn{}
	mockConnFunc := func(props map[string]string) (driver.Conn, error) {
		return mockConn, nil
	}
	context := limitless.NewConnectionContext(*host, props, nil, mockConnFunc, nil)

	err := limitlessRouterService.EstablishConnection(context)
	actualConn := context.GetConnection()

	assert.Nil(t, err)
	assert.Equal(t, mockConn, actualConn)
}

func TestLimitlessMonitorServiceEstablishConnect_GivenEmptyCacheAndNoWaitForRouterInfo(t *testing.T) {
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name:                "postgresql",
		property_util.LIMITLESS_WAIT_FOR_ROUTER_INFO.Name: "false",
	}

	clusterId := "someClusterId"

	mockHostListProviderService := &MockHostListProvider{}
	mockHostListProviderService.SetClusterId(clusterId)

	mockTargetDriver := &MockTargetDriver{}
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	mockPluginManager := driver_infrastructure.PluginManager(
		plugin_helpers.NewPluginManagerImpl(
			mockTargetDriver,
			props, driver_infrastructure.ConnectionProviderManager{},
			telemetryFactory))
	pluginServiceImpl, _ := plugin_helpers.NewPluginServiceImpl(mockPluginManager, mysql_driver.NewMySQLDriverDialect(), props, pgLimitlessTestDsn)
	pluginServiceImpl.SetHostListProvider(mockHostListProviderService)

	limitlessRouterService := limitless.NewLimitlessRouterServiceImplInternal(pluginServiceImpl, nil, props)

	defer limitless.LIMITLESS_ROUTER_CACHE.Clear()

	host, _ := host_info_util.NewHostInfoBuilder().SetHost("host1").SetWeight(9).Build()
	mockConn := &MockConn{}
	mockConnFunc := func(props map[string]string) (driver.Conn, error) {
		return mockConn, nil
	}
	context := limitless.NewConnectionContext(*host, props, nil, mockConnFunc, nil)

	err := limitlessRouterService.EstablishConnection(context)
	actualConn := context.GetConnection()

	assert.Nil(t, err)
	assert.Equal(t, mockConn, actualConn)
}

func TestLimitlessMonitorServiceEstablishConnect_MaxRetries(t *testing.T) {
	props := map[string]string{
		property_util.DRIVER_PROTOCOL.Name:                "postgresql",
		property_util.LIMITLESS_WAIT_FOR_ROUTER_INFO.Name: "true",
	}

	clusterId := "someClusterId"

	mockHostListProviderService := &MockHostListProvider{}
	mockHostListProviderService.SetClusterId(clusterId)

	mockTargetDriver := &MockTargetDriver{}
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	mockPluginManager := driver_infrastructure.PluginManager(
		plugin_helpers.NewPluginManagerImpl(
			mockTargetDriver,
			props,
			driver_infrastructure.ConnectionProviderManager{},
			telemetryFactory))
	pluginServiceImpl, _ := plugin_helpers.NewPluginServiceImpl(mockPluginManager, mysql_driver.NewMySQLDriverDialect(), props, pgLimitlessTestDsn)
	pluginServiceImpl.SetHostListProvider(mockHostListProviderService)

	mockLimitlessQueryHelper := &MockLimitlessQueryHelper{}

	limitlessRouterService := limitless.NewLimitlessRouterServiceImplInternal(pluginServiceImpl, mockLimitlessQueryHelper, props)

	defer limitless.LIMITLESS_ROUTER_CACHE.Clear()

	host, _ := host_info_util.NewHostInfoBuilder().SetHost("host1").SetWeight(9).Build()
	mockConn := &MockConn{}
	mockConnFunc := func(props map[string]string) (driver.Conn, error) {
		return mockConn, nil
	}
	context := limitless.NewConnectionContext(*host, props, nil, mockConnFunc, nil)

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
	conn driver.Conn,
	hostPortToMap int,
	props map[string]string) (hostInfoList []*host_info_util.HostInfo, err error) {
	queryHelper.queryForLimitlessRoutersCallCount++
	if queryHelper.queryForLimitlessRoutersFunc == nil {
		return nil, errors.New("someError")
	}
	return queryHelper.queryForLimitlessRoutersFunc()
}

func (queryHelper *MockLimitlessQueryHelper) SetQueryForLimitlessRoutersFunc(fn QueryForLimitlessRoutersFuncType) {
	queryHelper.queryForLimitlessRoutersFunc = fn
}
