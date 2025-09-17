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

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	mysql_driver "github.com/aws/aws-advanced-go-wrapper/go-mysql-driver"

	"github.com/stretchr/testify/assert"
)

var execFuncCalls = 0
var builder = host_info_util.NewHostInfoBuilder()
var host1, _ = builder.SetHost("mydatabase-instance-1.xyz.us-east-2.rds.amazonaws.com").SetPort(3306).SetRole(host_info_util.WRITER).Build()
var host2, _ = builder.SetHost("mydatabase-instance-2.xyz.us-east-2.rds.amazonaws.com").SetPort(3306).SetRole(host_info_util.READER).Build()
var mockConn = &MockConn{}

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

type MockMonitoringRdsHostListProvider struct {
	*driver_infrastructure.MonitoringRdsHostListProvider
}

func newTestMockMonitoringRdsHostListProvider(
	hostListProviderService driver_infrastructure.HostListProviderService,
	databaseDialect driver_infrastructure.TopologyAwareDialect,
	properties map[string]string,
	pluginService driver_infrastructure.PluginService) *MockMonitoringRdsHostListProvider {
	provider := &MockMonitoringRdsHostListProvider{
		MonitoringRdsHostListProvider: driver_infrastructure.NewMonitoringRdsHostListProvider(
			hostListProviderService,
			databaseDialect,
			properties,
			pluginService,
		),
	}
	_, _ = provider.RdsHostListProvider.GetClusterId()
	return provider
}

var mockMonitoringRdsHostListProvider *MockMonitoringRdsHostListProvider

type MockPluginServiceImpl struct {
	inTransactionResult    bool
	isCurrentHostNil       bool
	isInTransactionCounter int
	forceRefreshFails      bool
	isCurrentConnNil       bool
	*plugin_helpers.PluginServiceImpl
}

func newMockPluginServiceImpl(
	pluginManager driver_infrastructure.PluginManager,
	driverDialect driver_infrastructure.DriverDialect,
	props map[string]string,
	dsn string,
	inTransactionResult bool,
	isCurrentHostNil bool,
	forceRefreshFails bool,
	isCurrentConnNil bool) *MockPluginServiceImpl {
	pluginService, _ := plugin_helpers.NewPluginServiceImpl(pluginManager, driverDialect, props, dsn)
	pluginServiceImpl, _ := pluginService.(*plugin_helpers.PluginServiceImpl)
	return &MockPluginServiceImpl{
		inTransactionResult: inTransactionResult,
		PluginServiceImpl:   pluginServiceImpl,
		isCurrentHostNil:    isCurrentHostNil,
		forceRefreshFails:   forceRefreshFails,
		isCurrentConnNil:    isCurrentConnNil,
	}
}

func (t *MockPluginServiceImpl) GetHostListProvider() driver_infrastructure.HostListProvider {
	return mockMonitoringRdsHostListProvider
}

func (t *MockPluginServiceImpl) GetCurrentHostInfo() (*host_info_util.HostInfo, error) {
	if t.isCurrentHostNil {
		return nil, nil
	}
	return t.PluginServiceImpl.GetCurrentHostInfo()
}

func (t *MockPluginServiceImpl) IsInTransaction() bool {
	t.isInTransactionCounter++
	return t.inTransactionResult
}

func (t *MockPluginServiceImpl) GetCurrentConnection() driver.Conn {
	if t.isCurrentConnNil {
		return nil
	}
	return mockConn
}

func (t *MockPluginServiceImpl) ForceRefreshHostListWithTimeout(shouldVerifyWriter bool, timeoutMs int) (bool, error) {
	if t.forceRefreshFails {
		return false, nil
	}
	return t.PluginServiceImpl.ForceRefreshHostListWithTimeout(shouldVerifyWriter, timeoutMs)
}

type mockAuroraMysqlDialect struct {
	isRoleWriter bool
	driver_infrastructure.AuroraMySQLDatabaseDialect
}

func (t *mockAuroraMysqlDialect) GetHostListProvider(
	props map[string]string,
	hostListProviderService driver_infrastructure.HostListProviderService,
	pluginService driver_infrastructure.PluginService) driver_infrastructure.HostListProvider {
	return mockMonitoringRdsHostListProvider
}

func (t *mockAuroraMysqlDialect) GetWriterHostName(conn driver.Conn) (string, error) {
	return "mydatabase-instance-1", nil
}

func (t *mockAuroraMysqlDialect) GetTopology(conn driver.Conn, provider driver_infrastructure.HostListProvider) ([]*host_info_util.HostInfo, error) {
	return []*host_info_util.HostInfo{host1, host2}, nil
}

func (t *mockAuroraMysqlDialect) GetHostRole(conn driver.Conn) host_info_util.HostRole {
	if t.isRoleWriter {
		return host_info_util.WRITER
	}
	return host_info_util.READER
}

type mockDefaultPlugin struct {
	connectFails       bool
	acceptedStrategies []string
	plugins.DefaultPlugin
}

func (t *mockDefaultPlugin) Connect(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	if t.connectFails {
		return nil, errors.New("invalid connection")
	}
	return mockConn, nil
}

func (t *mockDefaultPlugin) ForceConnect(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	if t.connectFails {
		return nil, errors.New("invalid connection")
	}
	return mockConn, nil
}

func (t *mockDefaultPlugin) GetHostInfoByStrategy(
	role host_info_util.HostRole,
	strategy string,
	hosts []*host_info_util.HostInfo) (*host_info_util.HostInfo, error) {
	if slices.Contains(t.acceptedStrategies, strategy) {
		return hosts[0], nil
	}
	return nil, error_util.NewUnsupportedStrategyError("unsupported failover strategy")
}

func execFunc() (any, any, bool, error) {
	execFuncCalls++
	return 1, 0, true, nil
}

func initializeTest(
	props map[string]string,
	isInTransaction bool,
	isRoleWriter bool,
	isCurrentHostNil bool,
	connectFails bool,
	forceRefreshFails bool,
	isCurrentConnNil bool) (*MockFailoverPlugin, *MockPluginServiceImpl) {
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	mockPluginManager := driver_infrastructure.PluginManager(
		plugin_helpers.NewPluginManagerImpl(nil, props, driver_infrastructure.ConnectionProviderManager{}, telemetryFactory))
	pluginServiceImpl := newMockPluginServiceImpl(
		mockPluginManager,
		mysql_driver.MySQL2DriverDialect{},
		props,
		mysqlTestDsn,
		isInTransaction,
		isCurrentHostNil,
		forceRefreshFails,
		isCurrentConnNil)

	pluginServiceImpl.SetDialect(&mockAuroraMysqlDialect{isRoleWriter: isRoleWriter})
	mockPluginService := driver_infrastructure.PluginService(pluginServiceImpl)
	mySqlTestDsnProps, _ := utils.ParseDsn(mysqlTestDsn)

	hostListProviderService := driver_infrastructure.HostListProviderService(pluginServiceImpl)
	mockMonitoringRdsHostListProvider = newTestMockMonitoringRdsHostListProvider(
		hostListProviderService,
		&mockAuroraMysqlDialect{isRoleWriter: isRoleWriter},
		utils.CombineMaps(props, mySqlTestDsnProps),
		pluginServiceImpl)
	hostListProviderService.SetHostListProvider(mockMonitoringRdsHostListProvider)

	defaultPlugin := mockDefaultPlugin{
		connectFails:       connectFails,
		acceptedStrategies: []string{"random"},
		DefaultPlugin: plugins.DefaultPlugin{
			PluginService:       mockPluginService,
			DefaultConnProvider: mockPluginManager.GetDefaultConnectionProvider(),
			ConnProviderManager: mockPluginManager.GetConnectionProviderManager(),
		},
	}
	failoverPlugin, _ := plugins.NewFailoverPlugin(pluginServiceImpl, props)
	mockFailoverPlugin := &MockFailoverPlugin{FailoverPlugin: failoverPlugin}
	_ = mockPluginManager.Init(mockPluginService, []driver_infrastructure.ConnectionPlugin{mockFailoverPlugin, &defaultPlugin})
	_ = mockPluginManager.InitHostProvider(props, hostListProviderService)
	return mockFailoverPlugin, pluginServiceImpl
}

func setupTest() {
	execFuncCalls = 0
	mockConn.closeCounter = 0
}

func cleanupTest() {
	driver_infrastructure.MonitoringRdsHostListProviderClearCaches()
}

func TestFailoverWriter(t *testing.T) {
	setupTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
	}
	plugin, _ := initializeTest(props, false, true, false, false, false, false)
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

	cleanupTest()
}

func TestFailoverWriterInTransaction(t *testing.T) {
	setupTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
	}
	plugin, _ := initializeTest(props, true, true, false, false, false, false)
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

	cleanupTest()
}

func TestFailoverWriterFails(t *testing.T) {
	setupTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
		property_util.FAILOVER_TIMEOUT_MS.Name:     "3000",
	}
	plugin, _ := initializeTest(props, false, true, false, true, false, false)
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

	cleanupTest()
}

func TestFailoverWriterTopologyUpdateFailure(t *testing.T) {
	setupTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
	}
	plugin, _ := initializeTest(props, false, true, false, false, true, false)
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

	cleanupTest()
}

func TestFailoverWriterIncorrectRole(t *testing.T) {
	setupTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
	}
	plugin, _ := initializeTest(props, false, false, false, false, false, false)
	plugin.InitFailoverMode()

	err := plugin.Failover()
	if err != nil {
		assert.Equal(t, error_util.NewFailoverFailedError(error_util.GetMessage("Failover.unexpectedReaderRole", host1.Host, host_info_util.READER)), err)
	} else {
		assert.Fail(t, "Unexpected failover without error")
	}
	assert.Equal(t, 1, plugin.calledFailoverCount)
	assert.Equal(t, 1, plugin.calledFailoverWriterCount)
	assert.Equal(t, 0, plugin.calledFailoverReaderCount)

	cleanupTest()
}

func TestFailoverReader(t *testing.T) {
	setupTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
		property_util.FAILOVER_MODE.Name:           "strict-reader",
	}
	plugin, _ := initializeTest(props, false, false, false, false, false, false)
	plugin.InitFailoverMode()

	_, _, _, _ = plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, execFunc)
	err := plugin.Failover()
	if err != nil {
		assert.Equal(t, error_util.FailoverSuccessError, err)
	} else {
		assert.Fail(t, "Unexpected failover without error")
	}
	assert.Equal(t, 1, plugin.calledFailoverCount)
	assert.Equal(t, 0, plugin.calledFailoverWriterCount)
	assert.Equal(t, 1, plugin.calledFailoverReaderCount)

	cleanupTest()
}

func TestFailoverReaderInTransaction(t *testing.T) {
	setupTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
		property_util.FAILOVER_MODE.Name:           "strict-reader",
	}
	plugin, _ := initializeTest(props, true, false, false, false, false, false)
	plugin.InitFailoverMode()

	_, _, _, _ = plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, execFunc)
	err := plugin.Failover()
	if err != nil {
		assert.Equal(t, error_util.TransactionResolutionUnknownError, err)
	} else {
		assert.Fail(t, "Unexpected failover without error")
	}
	assert.Equal(t, 1, plugin.calledFailoverCount)
	assert.Equal(t, 0, plugin.calledFailoverWriterCount)
	assert.Equal(t, 1, plugin.calledFailoverReaderCount)

	cleanupTest()
}

func TestFailoverReaderFails(t *testing.T) {
	setupTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
		property_util.FAILOVER_MODE.Name:           "strict-reader",
	}
	plugin, _ := initializeTest(props, false, false, false, true, false, false)
	plugin.InitFailoverMode()

	_, _, _, _ = plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, execFunc)
	err := plugin.Failover()
	if err != nil {
		assert.Equal(t, error_util.NewTimeoutError(error_util.GetMessage("ClusterTopologyMonitorImpl.topologyNotUpdated", 1100)), err)
	} else {
		assert.Fail(t, "Unexpected failover without error")
	}
	assert.Equal(t, 1, plugin.calledFailoverCount)
	assert.Equal(t, 0, plugin.calledFailoverWriterCount)
	assert.Equal(t, 1, plugin.calledFailoverReaderCount)

	cleanupTest()
}

func TestFailoverReaderTopologyUpdateFailure(t *testing.T) {
	setupTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
		property_util.FAILOVER_MODE.Name:           "strict-reader",
	}
	plugin, _ := initializeTest(props, false, true, false, false, true, false)
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

	cleanupTest()
}

func TestFailoverReaderIncorrectRole(t *testing.T) {
	setupTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
		property_util.FAILOVER_MODE.Name:           "strict-reader",
		property_util.FAILOVER_TIMEOUT_MS.Name:     "3000",
	}
	plugin, _ := initializeTest(props, false, true, false, false, false, false)
	plugin.InitFailoverMode()

	_, _, _, _ = plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, execFunc)
	err := plugin.Failover()
	if err != nil {
		assert.Equal(t, error_util.NewFailoverFailedError(error_util.GetMessage("Failover.unableToConnectToReader")), err)
	} else {
		assert.Fail(t, "Unexpected failover without error")
	}
	assert.Equal(t, 1, plugin.calledFailoverCount)
	assert.Equal(t, 0, plugin.calledFailoverWriterCount)
	assert.Equal(t, 1, plugin.calledFailoverReaderCount)

	cleanupTest()
}

func TestFailoverReaderUnsupportedStrategy(t *testing.T) {
	setupTest()
	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name:                "false",
		property_util.DRIVER_PROTOCOL.Name:                        "mysql",
		property_util.FAILOVER_MODE.Name:                          "strict-reader",
		property_util.FAILOVER_READER_HOST_SELECTOR_STRATEGY.Name: "unsupported",
	}
	plugin, _ := initializeTest(props, false, false, false, false, false, false)
	plugin.InitFailoverMode()

	_, _, _, _ = plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, execFunc)
	err := plugin.Failover()
	if err != nil {
		assert.Equal(t, error_util.GetMessage("Failover.unableToConnectToReader"), err.Error())
	} else {
		assert.Fail(t, "Unexpected failover without error")
	}
	assert.Equal(t, 1, plugin.calledFailoverCount)
	assert.Equal(t, 0, plugin.calledFailoverWriterCount)
	assert.Equal(t, 1, plugin.calledFailoverReaderCount)

	cleanupTest()
}

func TestInvalidateCurrentConnectionWithNilConn(t *testing.T) {
	setupTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
	}
	plugin, pluginService := initializeTest(props, false, true, true, false, false, true)
	plugin.InitFailoverMode()

	plugin.InvalidateCurrentConnection()
	assert.Equal(t, 0, pluginService.isInTransactionCounter)
	assert.Equal(t, 0, mockConn.closeCounter)

	cleanupTest()
}

func TestInvalidateCurrentConnectionInTransaction(t *testing.T) {
	setupTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
	}
	plugin, pluginService := initializeTest(props, true, true, true, false, false, false)
	plugin.InitFailoverMode()

	plugin.InvalidateCurrentConnection()
	assert.Equal(t, 2, pluginService.isInTransactionCounter)
	assert.Equal(t, 1, mockConn.closeCounter)

	cleanupTest()
}

func TestInvalidateCurrentConnectionWithOpenConn(t *testing.T) {
	setupTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
	}
	plugin, pluginService := initializeTest(props, false, true, true, false, false, false)
	plugin.InitFailoverMode()

	plugin.InvalidateCurrentConnection()
	assert.Equal(t, 1, pluginService.isInTransactionCounter)
	assert.Equal(t, 1, mockConn.closeCounter)

	cleanupTest()
}

func TestExecuteWithFailoverDisabled(t *testing.T) {
	setupTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
	}
	plugin, _ := initializeTest(props, false, true, false, false, false, false)
	plugin.InitFailoverMode()

	_, _, _, _ = plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, execFunc)

	assert.Equal(t, 1, execFuncCalls)
	assert.Equal(t, 0, plugin.calledFailoverCount)
	assert.Equal(t, 0, mockConn.closeCounter)

	cleanupTest()
}
