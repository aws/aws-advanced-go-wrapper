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
	"fmt"
	"slices"
	"syscall"
	"testing"
	"time"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/plugins"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/services"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils/telemetry"
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
	) (driver_infrastructure.HostListProvider, error) {
		return failoverRdsHostListProvider, nil
	}
}

type FailoverMockPluginServiceImpl struct {
	inTransactionResult    bool
	isCurrentHostNil       bool
	isInTransactionCounter int
	forceRefreshFails      bool
	isCurrentConnNil       bool
	isRoleWriter           bool
	setUnavailableCalls    int
	*plugin_helpers.PluginServiceImpl
}

func (t *FailoverMockPluginServiceImpl) SetAvailability(hostAliases map[string]bool, availability host_info_util.HostAvailability) {
	if availability == host_info_util.UNAVAILABLE {
		t.setUnavailableCalls++
	}
	t.PluginServiceImpl.SetAvailability(hostAliases, availability)
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
		// Must be the constructor: the zero value has a nil errorHandler, so any
		// IsNetworkError/IsLoginError call through this dialect panics.
		mysql_driver.NewMySQLDriverDialect(),
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

	topologyUtils := driver_infrastructure.NewAuroraTopologyUtils(dialect, mysql_driver.NewMySQLDriverDialect(), combinedProps)
	failoverRdsHostListProvider = driver_infrastructure.NewRdsHostListProvider(
		hostListProviderService,
		topologyUtils,
		combinedProps,
		container,
		nil,
	)
	_, _ = failoverRdsHostListProvider.GetClusterId()
	hostListProviderService.SetHostListProvider(failoverRdsHostListProvider)

	defaultPlugin := failoverMockDefaultPlugin{
		connectFails:       connectFails,
		acceptedStrategies: []string{"random"},
		DefaultPlugin: plugins.DefaultPlugin{
			ServicesContainer: container,
		},
	}
	failoverPlugin, _ := plugins.NewFailoverPlugin(container, props, driver_infrastructure.FAILOVER_PLUGIN_CODE,
		func(p *plugins.FailoverPlugin) plugins.FailoverHandler { return plugins.NewRdsFailoverHandler(p) })
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
	assert.NoError(t, plugin.InitFailoverMode())

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
	assert.NoError(t, plugin.InitFailoverMode())

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
	assert.NoError(t, plugin.InitFailoverMode())

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
	assert.NoError(t, plugin.InitFailoverMode())

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
	assert.NoError(t, plugin.InitFailoverMode())

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
	assert.NoError(t, plugin.InitFailoverMode())

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
	assert.NoError(t, plugin.InitFailoverMode())

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
	assert.NoError(t, plugin.InitFailoverMode())

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
	assert.NoError(t, plugin.InitFailoverMode())

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
	assert.NoError(t, plugin.InitFailoverMode())

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
	assert.NoError(t, plugin.InitFailoverMode())

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
	assert.NoError(t, plugin.InitFailoverMode())

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
	assert.NoError(t, plugin.InitFailoverMode())

	plugin.InvalidateCurrentConnection()
	// One call: the state is read once into p.isInTransaction and branched on. It
	// used to be read twice (once for the branch, once for the assignment).
	assert.Equal(t, 1, pluginService.isInTransactionCounter)
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
	assert.NoError(t, plugin.InitFailoverMode())

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
	assert.NoError(t, plugin.InitFailoverMode())

	_, _, _, _ = plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, failoverExecFunc)

	assert.Equal(t, 1, failoverExecFuncCalls)
	assert.Equal(t, 0, plugin.calledFailoverCount)
	assert.Equal(t, 0, failoverMockConn.closeCounter)

	cleanupFailoverTest()
}

// failoverBadConnExecFunc reproduces what pgx hands the wrapper when it finds the
// socket already dead: a bare driver.ErrBadConn with the cause discarded.
func failoverBadConnExecFunc() (any, any, bool, error) {
	failoverExecFuncCalls++
	return nil, nil, false, driver.ErrBadConn
}

// driver.ErrBadConn is database/sql's stale-pooled-conn signal, so on its own it
// must not fail over. But a connection bound to an open transaction is actively in
// use and never reaped for pool hygiene, so ErrBadConn on a CLOSED connection
// inside a transaction can only be the server or network killing a live
// connection. Only that combination should fail over.
func TestExecuteBadConnFailsOverOnlyWhenConnDeadInTransaction(t *testing.T) {
	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
	}

	testCases := []struct {
		name           string
		inTransaction  bool
		connClosed     bool
		expectFailover bool
	}{
		{"dead conn in transaction fails over", true, true, true},
		{"live conn in transaction does not", true, false, false},
		{"dead conn outside a transaction does not", false, true, false},
		{"live conn outside a transaction does not", false, false, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			setupFailoverTest()
			failoverMockConn.isInvalid = tc.connClosed
			defer func() { failoverMockConn.isInvalid = false }()

			plugin, pluginService := initializeFailoverTest(t, props, tc.inTransaction, true, false, false, false, false)
			assert.NoError(t, plugin.InitFailoverMode())
			// isFailoverEnabled requires a non-empty host list; without this the gate
			// short-circuits before ever looking at the error.
			pluginService.AllHosts = []*host_info_util.HostInfo{failoverHost1, failoverHost2}

			_, _, _, err := plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, failoverBadConnExecFunc)

			if tc.expectFailover {
				// In a transaction the wrapper cannot know whether the transaction
				// committed, so it reports resolution-unknown rather than success.
				assert.True(t, error_util.IsType(err, error_util.TransactionResolutionUnknownErrorType),
					"expected a failover error, got %v", err)
			} else {
				assert.True(t, errors.Is(err, driver.ErrBadConn),
					"expected the raw ErrBadConn to pass through, got %v", err)
			}

			cleanupFailoverTest()
		})
	}
}

// lastErrorDealtWith must not latch. It guards against failing over twice on one
// error as it unwinds, but two independent driver.ErrBadConn occurrences are the
// same value, so errors.Is cannot tell them apart. Clearing it on a successful
// call keeps a later genuine failure from being silently swallowed.
func TestExecuteRepeatBadConnFailoverIsNotSwallowed(t *testing.T) {
	setupFailoverTest()
	failoverMockConn.isInvalid = true
	defer func() { failoverMockConn.isInvalid = false }()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
	}
	plugin, pluginService := initializeFailoverTest(t, props, true, true, false, false, false, false)
	assert.NoError(t, plugin.InitFailoverMode())
	pluginService.AllHosts = []*host_info_util.HostInfo{failoverHost1, failoverHost2}

	_, _, _, first := plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, failoverBadConnExecFunc)
	assert.True(t, error_util.IsType(first, error_util.TransactionResolutionUnknownErrorType),
		"first failure should fail over, got %v", first)

	// A successful call closes the propagating-error window.
	_, _, _, ok := plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, failoverExecFunc)
	assert.NoError(t, ok)

	_, _, _, second := plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, failoverBadConnExecFunc)
	assert.True(t, error_util.IsType(second, error_util.TransactionResolutionUnknownErrorType),
		"second failure must also fail over, not be swallowed by lastErrorDealtWith, got %v", second)

	cleanupFailoverTest()
}

// The ErrBadConn gate must read the LIVE transaction state only. p.isInTransaction
// is only ever assigned true, so if the gate consulted it a single transactional
// failover would make every later ErrBadConn look transactional and fail over
// outside a transaction.
func TestExecuteBadConnGateDoesNotLatchTransactionState(t *testing.T) {
	setupFailoverTest()
	failoverMockConn.isInvalid = true
	defer func() { failoverMockConn.isInvalid = false }()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
	}
	// Start in a transaction so the first failure latches p.isInTransaction.
	plugin, pluginService := initializeFailoverTest(t, props, true, true, false, false, false, false)
	assert.NoError(t, plugin.InitFailoverMode())
	pluginService.AllHosts = []*host_info_util.HostInfo{failoverHost1, failoverHost2}

	_, _, _, first := plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, failoverBadConnExecFunc)
	assert.True(t, error_util.IsType(first, error_util.TransactionResolutionUnknownErrorType),
		"in-transaction failure should fail over, got %v", first)

	// The transaction is over. A dead connection outside a transaction is
	// indistinguishable from ordinary pool staleness, so it must NOT fail over.
	pluginService.inTransactionResult = false
	_, _, _, ok := plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, failoverExecFunc)
	assert.NoError(t, ok)

	_, _, _, after := plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, failoverBadConnExecFunc)
	assert.True(t, errors.Is(after, driver.ErrBadConn),
		"outside a transaction ErrBadConn must pass through untouched, got %v", after)

	cleanupFailoverTest()
}

// p.isInTransaction snapshots the pre-failover transaction state because the
// connection switch clears the live flag. It must be re-snapshotted and consumed
// each time, or a transactional failover would latch it on and every later
// non-transactional failover would be misreported as resolution-unknown.
func TestFailoverInTransactionThenNotDoesNotMisreport(t *testing.T) {
	setupFailoverTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
	}
	plugin, pluginService := initializeFailoverTest(t, props, true, true, false, false, false, false)
	assert.NoError(t, plugin.InitFailoverMode())

	// A transactional failover: the caller cannot know whether the transaction
	// committed, so resolution-unknown is correct.
	first := plugin.Failover()
	assert.Equal(t, error_util.TransactionResolutionUnknownError, first)

	// A later failover with no transaction open must report plain success.
	pluginService.inTransactionResult = false
	second := plugin.Failover()
	assert.Equal(t, error_util.FailoverSuccessError, second,
		"a non-transactional failover must not inherit the previous transaction state")

	cleanupFailoverTest()
}

// A bare ErrBadConn does not say why the connection died, so the host must not be
// durably penalised on that guess - unlike an error that positively identifies a
// transport or host fault.
func TestBadConnFailoverDoesNotMarkHostUnavailable(t *testing.T) {
	setupFailoverTest()
	failoverMockConn.isInvalid = true
	defer func() { failoverMockConn.isInvalid = false }()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
	}
	plugin, pluginService := initializeFailoverTest(t, props, true, true, false, false, false, false)
	assert.NoError(t, plugin.InitFailoverMode())
	pluginService.AllHosts = []*host_info_util.HostInfo{failoverHost1, failoverHost2}

	_, _, _, err := plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, failoverBadConnExecFunc)
	assert.True(t, error_util.IsType(err, error_util.TransactionResolutionUnknownErrorType),
		"expected failover, got %v", err)
	assert.Equal(t, 0, pluginService.setUnavailableCalls,
		"ErrBadConn does not identify a host fault, so the host must not be marked UNAVAILABLE")

	cleanupFailoverTest()
}

// Contrast with TestBadConnFailoverDoesNotMarkHostUnavailable: an error that
// positively identifies a transport fault still marks the host unavailable.
func TestNetworkErrorFailoverMarksHostUnavailable(t *testing.T) {
	setupFailoverTest()

	props := map[string]string{
		property_util.ENABLE_CONNECT_FAILOVER.Name: "false",
		property_util.DRIVER_PROTOCOL.Name:         "mysql",
	}
	plugin, pluginService := initializeFailoverTest(t, props, true, true, false, false, false, false)
	assert.NoError(t, plugin.InitFailoverMode())
	pluginService.AllHosts = []*host_info_util.HostInfo{failoverHost1, failoverHost2}

	_, _, _, err := plugin.Execute(nil, utils.CONN_QUERY_CONTEXT, func() (any, any, bool, error) {
		failoverExecFuncCalls++
		return nil, nil, false, fmt.Errorf("write tcp: %w", syscall.EPIPE)
	})
	assert.True(t, error_util.IsType(err, error_util.TransactionResolutionUnknownErrorType),
		"expected failover, got %v", err)
	assert.Positive(t, pluginService.setUnavailableCalls,
		"a positively identified transport fault should mark the host UNAVAILABLE")

	cleanupFailoverTest()
}
