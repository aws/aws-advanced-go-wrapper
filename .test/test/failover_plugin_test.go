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
	"slices"
	"testing"
	"time"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/services"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	mysql_driver "github.com/aws/aws-advanced-go-wrapper/mysql-driver"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

var failoverExecFuncCalls = 0
var failoverBuilder = host_info_util.NewHostInfoBuilder()
var failoverHost1, _ = failoverBuilder.SetHost("mydatabase-instance-1.xyz.us-east-2.rds.amazonaws.com").SetPort(3306).SetRole(host_info_util.WRITER).Build()
var failoverHost2, _ = failoverBuilder.SetHost("mydatabase-instance-2.xyz.us-east-2.rds.amazonaws.com").SetPort(3306).SetRole(host_info_util.READER).Build()
var failoverMockConn = &MockConn{}

type MockFailoverPlugin struct {
	calledFailoverCount       int
	calledFailoverWriterCount int
	calledFailoverReaderCount int
	calledDealWithErrorCount  int
	*plugins.FailoverPlugin
}

func (p *MockFailoverPlugin) Failover() error {
	p.calledFailoverCount++
	if p.FailoverMode == plugins.MODE_STRICT_WRITER {
		return p.FailoverWriter()
	} else {
		return p.FailoverReader()
	}
}

func (p *MockFailoverPlugin) FailoverWriter() error {
	p.calledFailoverWriterCount++
	return p.FailoverPlugin.FailoverWriter()
}

func (p *MockFailoverPlugin) FailoverReader() error {
	p.calledFailoverReaderCount++
	return p.FailoverPlugin.FailoverReader()
}

func (p *MockFailoverPlugin) DealWithError(err error) error {
	p.calledDealWithErrorCount++
	return p.FailoverPlugin.DealWithError(err)
}

var failoverRdsHostListProvider *driver_infrastructure.RdsHostListProvider

type mockAuroraMysqlDialect struct {
	isRoleWriter bool
	driver_infrastructure.AuroraMySQLDatabaseDialect
}

func (t *mockAuroraMysqlDialect) GetHostListProviderSupplier() driver_infrastructure.HostListProviderSupplier {
	return func(
		props *utils.RWMap[string, string],
		initialDsn string,
		servicesContainer driver_infrastructure.ServicesContainer,
	) driver_infrastructure.HostListProvider {
		return failoverRdsHostListProvider
	}
}

type FailoverMockPluginServiceImpl struct {
	inTransactionResult    bool
	isCurrentHostNil       bool
	isInTransactionCounter int
	forceRefreshFails      bool
	isCurrentConnNil       bool
	isRoleWriter           bool
	*plugin_helpers.PluginServiceImpl
}

func newFailoverMockPluginServiceImpl(
	container driver_infrastructure.ServicesContainer,
	driverDialect driver_infrastructure.DriverDialect,
	props *utils.RWMap[string, string],
	dsn string,
	inTransactionResult bool,
	isCurrentHostNil bool,
	forceRefreshFails bool,
	isCurrentConnNil bool,
	isRoleWriter bool) *FailoverMockPluginServiceImpl {
	pluginService, _ := plugin_helpers.NewPluginServiceImpl(container, driverDialect, props, dsn)
	pluginServiceImpl, _ := pluginService.(*plugin_helpers.PluginServiceImpl)
	return &FailoverMockPluginServiceImpl{
		inTransactionResult: inTransactionResult,
		PluginServiceImpl:   pluginServiceImpl,
		isCurrentHostNil:    isCurrentHostNil,
		forceRefreshFails:   forceRefreshFails,
		isCurrentConnNil:    isCurrentConnNil,
		isRoleWriter:        isRoleWriter,
	}
}

func (t *FailoverMockPluginServiceImpl) GetHostListProvider() driver_infrastructure.HostListProvider {
	return failoverRdsHostListProvider
}

func (t *FailoverMockPluginServiceImpl) GetCurrentHostInfo() (*host_info_util.HostInfo, error) {
	if t.isCurrentHostNil {
		return nil, nil
	}
	return t.PluginServiceImpl.GetCurrentHostInfo()
}

func (t *FailoverMockPluginServiceImpl) IsInTransaction() bool {
	t.isInTransactionCounter++
	return t.inTransactionResult
}

func (t *FailoverMockPluginServiceImpl) GetCurrentConnection() driver.Conn {
	if t.isCurrentConnNil {
		return nil
	}
	return failoverMockConn
}

func (t *FailoverMockPluginServiceImpl) ForceRefreshHostListWithTimeout(shouldVerifyWriter bool, timeoutMs int) (bool, error) {
	if t.forceRefreshFails {
		return false, nil
	}
	return t.PluginServiceImpl.ForceRefreshHostListWithTimeout(shouldVerifyWriter, timeoutMs)
}

func (t *FailoverMockPluginServiceImpl) GetHostRole(_ driver.Conn) host_info_util.HostRole {
	if t.isRoleWriter {
		return host_info_util.WRITER
	}
	return host_info_util.READER
}

type failoverMockDefaultPlugin struct {
	connectFails       bool
	acceptedStrategies []string
	plugins.DefaultPlugin
}

func (t *failoverMockDefaultPlugin) Connect(
	_ *host_info_util.HostInfo,
	_ *utils.RWMap[string, string],
	_ bool,
	_ driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	if t.connectFails {
		return nil, errors.New("invalid connection")
	}
	return failoverMockConn, nil
}

func (t *failoverMockDefaultPlugin) ForceConnect(
	_ *host_info_util.HostInfo,
	_ *utils.RWMap[string, string],
	_ bool,
	_ driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	if t.connectFails {
		return nil, errors.New("invalid connection")
	}
	return failoverMockConn, nil
}

func (t *failoverMockDefaultPlugin) GetHostInfoByStrategy(
	_ host_info_util.HostRole,
	strategy string,
	hosts []*host_info_util.HostInfo) (*host_info_util.HostInfo, error) {
	if slices.Contains(t.acceptedStrategies, strategy) {
		return hosts[0], nil
	}
	return nil, error_util.NewUnsupportedStrategyError("unsupported failover strategy")
}

func failoverExecFunc() (any, any, bool, error) {
	failoverExecFuncCalls++
	return 1, 0, true, nil
}

func initializeFailoverTest(
	t *testing.T,
	propsMap map[string]string,
	isInTransaction bool,
	isRoleWriter bool,
	isCurrentHostNil bool,
	connectFails bool,
	forceRefreshFails bool,
	isCurrentConnNil bool) (*MockFailoverPlugin, *FailoverMockPluginServiceImpl) {
	ctrl := gomock.NewController(t)

	props := utils.NewRWMapFromMap(propsMap)
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)

	// Create storage service
	storage := services.NewExpiringStorage(5*time.Minute, nil)
	driver_infrastructure.RegisterDefaultStorageTypes(storage)

	// Create mock monitor service and topology monitor
	mockMonitorService := mock_driver_infrastructure.NewMockMonitorService(ctrl)
	mockTopologyMonitor := mock_driver_infrastructure.NewMockClusterTopologyMonitor(ctrl)

	// Configure mock monitor service
	mockMonitorService.EXPECT().RegisterMonitorType(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	mockMonitorService.EXPECT().RunIfAbsent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(mockTopologyMonitor, nil).AnyTimes()
	mockMonitorService.EXPECT().StopAndRemove(gomock.Any(), gomock.Any()).AnyTimes()

	// Configure mock topology monitor to return test hosts
	mockTopologyMonitor.EXPECT().ForceRefresh(gomock.Any(), gomock.Any()).
		Return([]*host_info_util.HostInfo{failoverHost1, failoverHost2}, nil).AnyTimes()

	// Create the services container
	container := &services.FullServicesContainer{
		Storage:   storage,
		Monitor:   mockMonitorService,
		Telemetry: telemetryFactory,
	}

	mockPluginManager := plugin_helpers.NewPluginManagerImpl(nil, container, props)
	container.PluginManager = mockPluginManager

	pluginServiceImpl := newFailoverMockPluginServiceImpl(
		container,
		mysql_driver.MySQLDriverDialect{},
		props,
		mysqlTestDsn,
		isInTransaction,
		isCurrentHostNil,
		forceRefreshFails,
		isCurrentConnNil,
		isRoleWriter)

	dialect := &mockAuroraMysqlDialect{isRoleWriter: isRoleWriter}
	pluginServiceImpl.SetDialect(dialect)
	mockPluginService := driver_infrastructure.PluginService(pluginServiceImpl)
	container.PluginService = mockPluginService

	mySqlTestDsnProps, _ := property_util.ParseDsn(mysqlTestDsn)
	combinedProps := utils.CombineRWMaps(props, mySqlTestDsnProps)

	hostListProviderService := driver_infrastructure.HostListProviderService(pluginServiceImpl)
	container.HostListProviderService = hostListProviderService

	topologyUtils := driver_infrastructure.NewAuroraTopologyUtils(dialect)
	failoverRdsHostListProvider = driver_infrastructure.NewRdsHostListProvider(
		hostListProviderService,
		topologyUtils,
		combinedProps,
		container,
	)
	_, _ = failoverRdsHostListProvider.GetClusterId()
	hostListProviderService.SetHostListProvider(failoverRdsHostListProvider)

	defaultPlugin := failoverMockDefaultPlugin{
		connectFails:       connectFails,
		acceptedStrategies: []string{"random"},
		DefaultPlugin: plugins.DefaultPlugin{
			ServicesContainer:   container,
			ConnProviderManager: mockPluginManager.GetConnectionProviderManager(),
		},
	}
	failoverPlugin, _ := plugins.NewFailoverPlugin(container, props)
	mockFailoverPlugin := &MockFailoverPlugin{FailoverPlugin: failoverPlugin}
	_ = mockPluginManager.Init([]driver_infrastructure.ConnectionPlugin{mockFailoverPlugin, &defaultPlugin})
	_ = mockPluginManager.InitHostProvider(props, hostListProviderService)
	return mockFailoverPlugin, pluginServiceImpl
}

func setupFailoverTest() {
	failoverExecFuncCalls = 0
	failoverMockConn.closeCounter = 0
}

func cleanupFailoverTest() {
	// RdsHostListProvider caches are now in StorageService, no global cleanup needed
}

func TestFailoverWriter(t *testing.T) {
	setupFailoverTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
	}
	plugin, _ := initializeFailoverTest(t, props, false, true, false, false, false, false)
	plugin.InitFailoverMode()

	err := plugin.Failover()
	if err != nil {
		assert.Equal(t, error_util.FailoverSuccessError, err)
	} else {
		assert.Fail(t, "Unexpected failover without error")
	}
	assert.Equal(t, 1, plugin.calledFailoverCount)
	assert.Equal(t, 1, plugin.calledFailoverWriterCount)
	assert.Equal(t, 0, plugin.calledFailoverReaderCount)

	cleanupFailoverTest()
}

func TestFailoverWriterInTransaction(t *testing.T) {
	setupFailoverTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
	}
	plugin, _ := initializeFailoverTest(t, props, true, true, false, false, false, false)
	plugin.InitFailoverMode()

	err := plugin.Failover()
	if err != nil {
		assert.Equal(t, error_util.TransactionResolutionUnknownError, err)
	} else {
		assert.Fail(t, "Unexpected failover without error")
	}
	assert.Equal(t, 1, plugin.calledFailoverCount)
	assert.Equal(t, 1, plugin.calledFailoverWriterCount)
	assert.Equal(t, 0, plugin.calledFailoverReaderCount)

	cleanupFailoverTest()
}

func TestFailoverWriterFails(t *testing.T) {
	setupFailoverTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
		property_util.FAILOVER_TIMEOUT_MS.Name:     "3000",
	}
	plugin, _ := initializeFailoverTest(t, props, false, true, false, true, false, false)
	plugin.InitFailoverMode()

	err := plugin.Failover()
	if err != nil {
		// The mock monitor returns hosts successfully, so ForceRefreshHostListWithTimeout succeeds.
		// The failover then tries to connect to the writer candidate, which fails with "invalid connection".
		assert.Equal(t, error_util.NewFailoverFailedError("invalid connection"), err)
	} else {
		assert.Fail(t, "Unexpected failover without error")
	}
	assert.Equal(t, 1, plugin.calledFailoverCount)
	assert.Equal(t, 1, plugin.calledFailoverWriterCount)
	assert.Equal(t, 0, plugin.calledFailoverReaderCount)

	cleanupFailoverTest()
}

func TestFailoverWriterTopologyUpdateFailure(t *testing.T) {
	setupFailoverTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
	}
	plugin, _ := initializeFailoverTest(t, props, false, true, false, false, true, false)
	plugin.InitFailoverMode()

	err := plugin.Failover()
	if err != nil {
		assert.Equal(t, error_util.NewFailoverFailedError(error_util.GetMessage("Failover.unableToRefreshHostList")), err)
	} else {
		assert.Fail(t, "Unexpected failover without error")
	}
	assert.Equal(t, 1, plugin.calledFailoverCount)
	assert.Equal(t, 1, plugin.calledFailoverWriterCount)
	assert.Equal(t, 0, plugin.calledFailoverReaderCount)

	cleanupFailoverTest()
}

func TestFailoverWriterIncorrectRole(t *testing.T) {
	setupFailoverTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
	}
	plugin, _ := initializeFailoverTest(t, props, false, false, false, false, false, false)
	plugin.InitFailoverMode()

	err := plugin.Failover()
	if err != nil {
		assert.Equal(t, error_util.NewFailoverFailedError(error_util.GetMessage("Failover.unexpectedReaderRole", failoverHost1.Host, host_info_util.READER)), err)
	} else {
		assert.Fail(t, "Unexpected failover without error")
	}
	assert.Equal(t, 1, plugin.calledFailoverCount)
	assert.Equal(t, 1, plugin.calledFailoverWriterCount)
	assert.Equal(t, 0, plugin.calledFailoverReaderCount)

	cleanupFailoverTest()
}

func TestFailoverReader(t *testing.T) {
	setupFailoverTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
		property_util.FAILOVER_MODE.Name:           "strict-reader",
	}
	plugin, _ := initializeFailoverTest(t, props, false, false, false, false, false, false)
	plugin.InitFailoverMode()

	_, _, _, _ = plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, failoverExecFunc)
	err := plugin.Failover()
	if err != nil {
		assert.Equal(t, error_util.FailoverSuccessError, err)
	} else {
		assert.Fail(t, "Unexpected failover without error")
	}
	assert.Equal(t, 1, plugin.calledFailoverCount)
	assert.Equal(t, 0, plugin.calledFailoverWriterCount)
	assert.Equal(t, 1, plugin.calledFailoverReaderCount)

	cleanupFailoverTest()
}

func TestFailoverReaderInTransaction(t *testing.T) {
	setupFailoverTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
		property_util.FAILOVER_MODE.Name:           "strict-reader",
	}
	plugin, _ := initializeFailoverTest(t, props, true, false, false, false, false, false)
	plugin.InitFailoverMode()

	_, _, _, _ = plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, failoverExecFunc)
	err := plugin.Failover()
	if err != nil {
		assert.Equal(t, error_util.TransactionResolutionUnknownError, err)
	} else {
		assert.Fail(t, "Unexpected failover without error")
	}
	assert.Equal(t, 1, plugin.calledFailoverCount)
	assert.Equal(t, 0, plugin.calledFailoverWriterCount)
	assert.Equal(t, 1, plugin.calledFailoverReaderCount)

	cleanupFailoverTest()
}

func TestFailoverReaderFails(t *testing.T) {
	setupFailoverTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
		property_util.FAILOVER_MODE.Name:           "strict-reader",
		property_util.FAILOVER_TIMEOUT_MS.Name:     "3000",
	}
	plugin, _ := initializeFailoverTest(t, props, false, false, false, true, false, false)
	plugin.InitFailoverMode()

	_, _, _, _ = plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, failoverExecFunc)
	err := plugin.Failover()
	if err != nil {
		// All connection attempts fail, so the reader failover loop runs until timeout.
		assert.Equal(t, error_util.NewFailoverFailedError(error_util.GetMessage("Failover.unableToConnectToReader")), err)
	} else {
		assert.Fail(t, "Unexpected failover without error")
	}
	assert.Equal(t, 1, plugin.calledFailoverCount)
	assert.Equal(t, 0, plugin.calledFailoverWriterCount)
	assert.Equal(t, 1, plugin.calledFailoverReaderCount)

	cleanupFailoverTest()
}

func TestFailoverReaderTopologyUpdateFailure(t *testing.T) {
	setupFailoverTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
		property_util.FAILOVER_MODE.Name:           "strict-reader",
	}
	plugin, _ := initializeFailoverTest(t, props, false, true, false, false, true, false)
	plugin.InitFailoverMode()

	err := plugin.Failover()
	if err != nil {
		assert.Equal(t, error_util.NewFailoverFailedError(error_util.GetMessage("Failover.failoverReaderUnableToRefreshHostList")), err)
	} else {
		assert.Fail(t, "Unexpected failover without error")
	}
	assert.Equal(t, 1, plugin.calledFailoverCount)
	assert.Equal(t, 0, plugin.calledFailoverWriterCount)
	assert.Equal(t, 1, plugin.calledFailoverReaderCount)

	cleanupFailoverTest()
}

func TestFailoverReaderIncorrectRole(t *testing.T) {
	setupFailoverTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
		property_util.FAILOVER_MODE.Name:           "strict-reader",
		property_util.FAILOVER_TIMEOUT_MS.Name:     "3000",
	}
	plugin, _ := initializeFailoverTest(t, props, false, true, false, false, false, false)
	plugin.InitFailoverMode()

	_, _, _, _ = plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, failoverExecFunc)
	err := plugin.Failover()
	if err != nil {
		assert.Equal(t, error_util.NewFailoverFailedError(error_util.GetMessage("Failover.unableToConnectToReader")), err)
	} else {
		assert.Fail(t, "Unexpected failover without error")
	}
	assert.Equal(t, 1, plugin.calledFailoverCount)
	assert.Equal(t, 0, plugin.calledFailoverWriterCount)
	assert.Equal(t, 1, plugin.calledFailoverReaderCount)

	cleanupFailoverTest()
}

func TestFailoverReaderUnsupportedStrategy(t *testing.T) {
	setupFailoverTest()
	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name:                "false",
		property_util.DRIVER_PROTOCOL.Name:                        "mysql",
		property_util.FAILOVER_MODE.Name:                          "strict-reader",
		property_util.FAILOVER_READER_HOST_SELECTOR_STRATEGY.Name: "unsupported",
	}
	plugin, _ := initializeFailoverTest(t, props, false, false, false, false, false, false)
	plugin.InitFailoverMode()

	_, _, _, _ = plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, failoverExecFunc)
	err := plugin.Failover()
	if err != nil {
		assert.Equal(t, error_util.GetMessage("Failover.unableToConnectToReader"), err.Error())
	} else {
		assert.Fail(t, "Unexpected failover without error")
	}
	assert.Equal(t, 1, plugin.calledFailoverCount)
	assert.Equal(t, 0, plugin.calledFailoverWriterCount)
	assert.Equal(t, 1, plugin.calledFailoverReaderCount)

	cleanupFailoverTest()
}

func TestInvalidateCurrentConnectionWithNilConn(t *testing.T) {
	setupFailoverTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
	}
	plugin, pluginService := initializeFailoverTest(t, props, false, true, true, false, false, true)
	plugin.InitFailoverMode()

	plugin.InvalidateCurrentConnection()
	assert.Equal(t, 0, pluginService.isInTransactionCounter)
	assert.Equal(t, 0, failoverMockConn.closeCounter)

	cleanupFailoverTest()
}

func TestInvalidateCurrentConnectionInTransaction(t *testing.T) {
	setupFailoverTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
	}
	plugin, pluginService := initializeFailoverTest(t, props, true, true, true, false, false, false)
	plugin.InitFailoverMode()

	plugin.InvalidateCurrentConnection()
	assert.Equal(t, 2, pluginService.isInTransactionCounter)
	assert.Equal(t, 1, failoverMockConn.closeCounter)

	cleanupFailoverTest()
}

func TestInvalidateCurrentConnectionWithOpenConn(t *testing.T) {
	setupFailoverTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
	}
	plugin, pluginService := initializeFailoverTest(t, props, false, true, true, false, false, false)
	plugin.InitFailoverMode()

	plugin.InvalidateCurrentConnection()
	assert.Equal(t, 1, pluginService.isInTransactionCounter)
	assert.Equal(t, 1, failoverMockConn.closeCounter)

	cleanupFailoverTest()
}

func TestExecuteWithFailoverDisabled(t *testing.T) {
	setupFailoverTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
	}
	plugin, _ := initializeFailoverTest(t, props, false, true, false, false, false, false)
	plugin.InitFailoverMode()

	_, _, _, _ = plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, failoverExecFunc)

	assert.Equal(t, 1, failoverExecFuncCalls)
	assert.Equal(t, 0, plugin.calledFailoverCount)
	assert.Equal(t, 0, failoverMockConn.closeCounter)

	cleanupFailoverTest()
}
