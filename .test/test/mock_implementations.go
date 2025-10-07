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
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"time"

	auth_helpers "github.com/aws/aws-advanced-go-wrapper/auth-helpers"
	aws_secrets_manager "github.com/aws/aws-advanced-go-wrapper/aws-secrets-manager"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/efm"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/limitless"
	"github.com/aws/aws-advanced-go-wrapper/awssql/region_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

var mysqlTestDsn = "someUser:somePassword@tcp(mydatabase.cluster-xyz.us-east-2.rds.amazonaws.com:3306)/myDatabase?foo=bar&pop=snap"
var pgTestDsn = "postgres://someUser:somePassword@mydatabase.cluster-xyz.us-east-2.rds.amazonaws.com:5432/pgx_test?sslmode=disable&foo=bar"

var defaultPluginFactoryByCode = map[string]driver_infrastructure.ConnectionPluginFactory{
	"failover":      plugins.NewFailoverPluginFactory(),
	"efm":           efm.NewHostMonitoringPluginFactory(),
	"limitless":     limitless.NewLimitlessPluginFactory(),
	"executionTime": plugins.NewExecutionTimePluginFactory(),
}

var testPluginCode = "test"

type TestPlugin struct {
	calls      *[]string
	id         int
	connection driver.Conn
	error      error
	isBefore   bool
}

func (t TestPlugin) GetPluginCode() string {
	return testPluginCode
}

func (t TestPlugin) GetSubscribedMethods() []string {
	switch t.id {
	case 1:
		return []string{"*"}
	case 2:
		return []string{"callA", "callB"}
	case 3:
		return []string{"callA", plugin_helpers.FORCE_CONNECT_METHOD, plugin_helpers.CONNECT_METHOD}
	default:
		return []string{"*"}
	}
}

func (t TestPlugin) Execute(_ driver.Conn, _ string, executeFunc driver_infrastructure.ExecuteFunc, _ ...any) (any, any, bool, error) {
	*t.calls = append(*t.calls, fmt.Sprintf("%s%v:before", reflect.TypeOf(t), t.id))
	if t.isBefore && t.error != nil {
		return nil, nil, false, t.error
	}
	result, _, _, err := executeFunc()
	*t.calls = append(*t.calls, fmt.Sprintf("%s%v:after", reflect.TypeOf(t), t.id))
	if !t.isBefore && t.error != nil {
		return nil, nil, false, t.error
	}
	return result, nil, true, err
}

func (t TestPlugin) Connect(
	_ *host_info_util.HostInfo,
	properties *utils.RWMap[string, string],
	_ bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	*t.calls = append(*t.calls, fmt.Sprintf("%s%v:before connect", reflect.TypeOf(t), t.id))
	if t.isBefore && t.error != nil {
		return nil, t.error
	}
	if t.connection != nil {
		*t.calls = append(*t.calls, fmt.Sprintf("%s%v:connection", reflect.TypeOf(t), t.id))
		return t.connection, nil
	}
	conn, err := connectFunc(properties)
	if !t.isBefore && t.error != nil {
		return nil, t.error
	}
	*t.calls = append(*t.calls, fmt.Sprintf("%s%v:after connect", reflect.TypeOf(t), t.id))
	return conn, err
}

func (t TestPlugin) ForceConnect(
	_ *host_info_util.HostInfo,
	properties *utils.RWMap[string, string],
	_ bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	*t.calls = append(*t.calls, fmt.Sprintf("%s%v:before forceConnect", reflect.TypeOf(t), t.id))
	if t.connection != nil {
		*t.calls = append(*t.calls, fmt.Sprintf("%s%v:forced connection", reflect.TypeOf(t), t.id))
		return t.connection, nil
	}
	conn, err := connectFunc(properties)
	*t.calls = append(*t.calls, fmt.Sprintf("%s%v:after forceConnect", reflect.TypeOf(t), t.id))
	return conn, err
}

func (t TestPlugin) AcceptsStrategy(_ string) bool {
	return false
}

func (t TestPlugin) GetHostInfoByStrategy(
	_ host_info_util.HostRole,
	_ string,
	_ []*host_info_util.HostInfo) (*host_info_util.HostInfo, error) {
	*t.calls = append(*t.calls, fmt.Sprintf("%s%v:before GetHostInfoByStrategy", reflect.TypeOf(t), t.id))
	*t.calls = append(*t.calls, fmt.Sprintf("%s%v:after GetHostInfoByStrategy", reflect.TypeOf(t), t.id))
	return nil, nil
}

func (t TestPlugin) GetHostSelectorStrategy(_ string) (driver_infrastructure.HostSelector, error) {
	return nil, nil
}

func (t TestPlugin) NotifyConnectionChanged(_ map[driver_infrastructure.HostChangeOptions]bool) driver_infrastructure.OldConnectionSuggestedAction {
	return driver_infrastructure.NO_OPINION
}

func (t TestPlugin) NotifyHostListChanged(_ map[string]map[driver_infrastructure.HostChangeOptions]bool) {
	// Do nothing
}

func (t TestPlugin) InitHostProvider(
	_ *utils.RWMap[string, string],
	_ driver_infrastructure.HostListProviderService,
	_ func() error) error {
	// Do nothing
	return nil
}

type MockTargetDriver struct{}

func (m MockTargetDriver) Open(_ string) (driver.Conn, error) {
	return nil, nil
}

type MockDriverConnection struct {
	id       int
	IsClosed bool
}

func (m *MockDriverConnection) Prepare(_ string) (driver.Stmt, error) {
	// Do nothing.
	return nil, nil
}

func (m *MockDriverConnection) Close() error {
	m.IsClosed = true
	// Do nothing.
	return nil
}

func (m *MockDriverConnection) Begin() (driver.Tx, error) {
	// Do nothing.
	return nil, nil
}

func CreateTestPlugin(calls *[]string, id int, connection driver.Conn, err error, isBefore bool) driver_infrastructure.ConnectionPlugin {
	if calls == nil {
		calls = &[]string{}
	}
	testPlugin := &TestPlugin{calls: calls, id: id, connection: connection, error: err, isBefore: isBefore}
	return testPlugin
}

type MockHostListProvider struct {
	clusterId string
}

func (m *MockHostListProvider) CreateHost(_ string, _ host_info_util.HostRole, _ float64, _ float64, _ time.Time) *host_info_util.HostInfo {
	return nil
}

func (m *MockHostListProvider) ForceRefresh(_ driver.Conn) ([]*host_info_util.HostInfo, error) {
	return nil, nil
}

func (m *MockHostListProvider) GetClusterId() (string, error) {
	return m.clusterId, nil
}

func (m *MockHostListProvider) SetClusterId(clusterId string) {
	m.clusterId = clusterId
}

func (m *MockHostListProvider) GetHostRole(_ driver.Conn) host_info_util.HostRole {
	return host_info_util.UNKNOWN
}

func (m *MockHostListProvider) IdentifyConnection(_ driver.Conn) (*host_info_util.HostInfo, error) {
	hostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("hostA").Build()
	return hostInfo, nil
}

func (m *MockHostListProvider) IsStaticHostListProvider() bool {
	return false
}

func (m *MockHostListProvider) Refresh(_ driver.Conn) ([]*host_info_util.HostInfo, error) {
	hostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("hostA").Build()
	return []*host_info_util.HostInfo{hostInfo}, nil
}

type MockPluginManager struct {
	driver_infrastructure.PluginManager
	Changes           map[string]map[driver_infrastructure.HostChangeOptions]bool
	ForceConnectProps *utils.RWMap[string, string]
}

func (pluginManager *MockPluginManager) NotifyHostListChanged(changes map[string]map[driver_infrastructure.HostChangeOptions]bool) {
	pluginManager.Changes = changes
}

func (pluginManager *MockPluginManager) ForceConnect(
	_ *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	_ bool) (driver.Conn, error) {
	pluginManager.ForceConnectProps = props
	return &MockDriverConnection{}, nil
}

type MockConn struct {
	queryResult        driver.Rows
	execResult         driver.Result
	beginResult        driver.Tx
	prepareResult      driver.Stmt
	throwError         bool
	closeCounter       int
	execContextCounter int
	isInvalid          bool
}

func (m *MockConn) Close() error {
	m.closeCounter++
	return nil
}

func (m *MockConn) Prepare(_ string) (driver.Stmt, error) {
	return m.prepareResult, nil
}

func (m *MockConn) Begin() (driver.Tx, error) {
	return m.beginResult, nil
}

func (m *MockConn) QueryContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	if m.queryResult != nil || !m.throwError {
		return m.queryResult, nil
	}
	return nil, errors.New("test error")
}

func (m *MockConn) ExecContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
	m.execContextCounter++
	if !m.throwError {
		return m.execResult, nil
	}
	return nil, errors.New("test error")
}

func (m *MockConn) IsValid() bool {
	return !m.isInvalid
}

func (m *MockConn) updateQueryRow(columns []string, row []driver.Value) {
	testRow := MockRows{columns: columns, row: row, throwNextError: -1}
	m.queryResult = &testRow
}

func (m *MockConn) updateThrowError(throwError bool) {
	m.throwError = throwError
}

func (m *MockConn) updateQueryRowSingleUse(columns []string, row []driver.Value) {
	testRow := MockRows{columns: columns, row: row, throwNextError: 1}
	m.queryResult = &testRow
}

type MockStmt struct {
}

func (a MockStmt) Close() error {
	return nil
}

func (a MockStmt) Exec(_ []driver.Value) (driver.Result, error) {
	return MockResult{}, errors.New("MockStmt error")
}

func (a MockStmt) NumInput() int {
	return 1
}

func (a MockStmt) Query(_ []driver.Value) (driver.Rows, error) {
	return &MockRows{[]string{"column"}, []driver.Value{"test"}, -1}, nil
}

type MockResult struct {
}

func (m MockResult) LastInsertId() (int64, error) {
	return 1, errors.New("MockResult error")
}

func (m MockResult) RowsAffected() (int64, error) {
	return 1, errors.New("MockResult error")
}

type MockTx struct {
	commitCounter   *int
	rollbackCounter *int
}

func NewMockTx() *MockTx {
	return &MockTx{commitCounter: new(int), rollbackCounter: new(int)}
}

func (a MockTx) Commit() error {
	if a.commitCounter != nil {
		*a.commitCounter++
	}
	return errors.New("MockTx error")
}

func (a MockTx) Rollback() error {
	if a.rollbackCounter != nil {
		*a.rollbackCounter++
	}
	return errors.New("MockTx error")
}

type MockPluginService struct {
	conn          driver.Conn
	PluginManager driver_infrastructure.PluginManager
}

func (p *MockPluginService) UpdateState(_ string, _ ...any) {
}

func (p *MockPluginService) GetHostSelectorStrategy(_ string) (hostSelector driver_infrastructure.HostSelector, err error) {
	return nil, nil
}

func (p *MockPluginService) GetCurrentConnection() driver.Conn {
	return p.conn
}

func (p *MockPluginService) GetCurrentConnectionRef() *driver.Conn {
	return nil
}

func (p *MockPluginService) SetCurrentConnection(
	conn driver.Conn,
	_ *host_info_util.HostInfo,
	_ driver_infrastructure.ConnectionPlugin,
) error {
	p.conn = conn
	return nil
}

func (p *MockPluginService) GetInitialConnectionHostInfo() *host_info_util.HostInfo {
	return nil
}

func (p *MockPluginService) GetCurrentHostInfo() (*host_info_util.HostInfo, error) {
	return nil, nil
}

func (p *MockPluginService) GetHosts() []*host_info_util.HostInfo {
	return nil
}

func (p *MockPluginService) AcceptsStrategy(_ string) bool {
	return false
}

func (p *MockPluginService) GetHostInfoByStrategy(_ host_info_util.HostRole, _ string, _ []*host_info_util.HostInfo) (*host_info_util.HostInfo, error) {
	return nil, nil
}

func (p *MockPluginService) GetHostRole(_ driver.Conn) host_info_util.HostRole {
	return "" // replace with a default if available
}

func (p *MockPluginService) SetAvailability(_ map[string]bool, _ host_info_util.HostAvailability) {
}

func (p *MockPluginService) IsInTransaction() bool {
	return false
}

func (p *MockPluginService) SetInTransaction(_ bool) {}

func (p *MockPluginService) GetCurrentTx() driver.Tx {
	return nil
}

func (p *MockPluginService) SetCurrentTx(_ driver.Tx) {}

func (p *MockPluginService) CreateHostListProvider(_ *utils.RWMap[string, string]) driver_infrastructure.HostListProvider {
	return nil
}

func (p *MockPluginService) SetHostListProvider(_ driver_infrastructure.HostListProvider) {
}

func (p *MockPluginService) SetInitialConnectionHostInfo(_ *host_info_util.HostInfo) {}

func (p *MockPluginService) IsStaticHostListProvider() bool {
	return false
}

func (p *MockPluginService) GetHostListProvider() driver_infrastructure.HostListProvider {
	return nil
}

func (p *MockPluginService) RefreshHostList(_ driver.Conn) error {
	return nil
}

func (p *MockPluginService) ForceRefreshHostList(_ driver.Conn) error {
	return nil
}

func (p *MockPluginService) ForceRefreshHostListWithTimeout(_ bool, _ int) (bool, error) {
	return false, nil
}

func (p *MockPluginService) GetUpdatedHostListWithTimeout(_ bool, _ int) ([]*host_info_util.HostInfo, error) {
	return nil, nil
}

func (p *MockPluginService) Connect(_ *host_info_util.HostInfo, _ *utils.RWMap[string, string], _ driver_infrastructure.ConnectionPlugin) (driver.Conn, error) {
	return nil, nil
}

func (p *MockPluginService) ForceConnect(_ *host_info_util.HostInfo, _ *utils.RWMap[string, string]) (driver.Conn, error) {
	return nil, nil
}

func (p *MockPluginService) GetDialect() driver_infrastructure.DatabaseDialect {
	return nil
}

func (p *MockPluginService) SetDialect(_ driver_infrastructure.DatabaseDialect) {}

func (p *MockPluginService) UpdateDialect(_ driver.Conn) {}

func (p *MockPluginService) GetTargetDriverDialect() driver_infrastructure.DriverDialect {
	return nil
}

func (p *MockPluginService) IdentifyConnection(_ driver.Conn) (*host_info_util.HostInfo, error) {
	return nil, nil
}

func (p *MockPluginService) FillAliases(_ driver.Conn, _ *host_info_util.HostInfo) {}

func (p *MockPluginService) GetConnectionProvider() driver_infrastructure.ConnectionProvider {
	return nil
}

func (p *MockPluginService) GetProperties() *utils.RWMap[string, string] {
	return nil
}

func (p *MockPluginService) IsNetworkError(_ error) bool {
	return false
}

func (p *MockPluginService) IsLoginError(_ error) bool {
	return false
}

func (p *MockPluginService) GetTelemetryContext() context.Context {
	return p.PluginManager.GetTelemetryContext()
}

func (p *MockPluginService) GetTelemetryFactory() telemetry.TelemetryFactory {
	return p.PluginManager.GetTelemetryFactory()
}

func (p *MockPluginService) SetTelemetryContext(ctx context.Context) {
	p.PluginManager.SetTelemetryContext(ctx)
}

func (p *MockPluginService) ResetSession() {
}

func (p *MockPluginService) GetBgStatus(_ string) (driver_infrastructure.BlueGreenStatus, bool) {
	return driver_infrastructure.BlueGreenStatus{}, true
}

func (p *MockPluginService) SetBgStatus(_ driver_infrastructure.BlueGreenStatus, _ string) {
}

func (p *MockPluginService) IsPluginInUse(_ string) bool {
	return false
}

type MockDriverConn struct {
	driver.Conn
}

type MockRows struct {
	columns        []string
	row            []driver.Value
	throwNextError int
}

func (t *MockRows) Columns() []string {
	return t.columns
}

func (t *MockRows) Close() error {
	return nil
}

func (t *MockRows) Next(dest []driver.Value) error {
	if len(t.row) < 1 {
		return errors.New("test error")
	}
	for i := range dest {
		dest[i] = t.row[i]
	}
	switch t.throwNextError {
	case 1:
		t.throwNextError = 0
	case 0:
		t.columns = nil
		t.row = nil
		return errors.New("test error")
	}
	return nil
}

type MockRdsHostListProviderService struct {
}

func (m *MockRdsHostListProviderService) IsStaticHostListProvider() bool {
	return false
}

func (m *MockRdsHostListProviderService) CreateHostListProvider(_ *utils.RWMap[string, string]) driver_infrastructure.HostListProvider {
	return nil
}

func (m *MockRdsHostListProviderService) GetCurrentConnection() driver.Conn {
	return nil
}

func (m *MockRdsHostListProviderService) GetDialect() driver_infrastructure.DatabaseDialect {
	return nil
}

func (m *MockRdsHostListProviderService) GetHostListProvider() driver_infrastructure.HostListProvider {
	return nil
}

func (m *MockRdsHostListProviderService) GetInitialConnectionHostInfo() *host_info_util.HostInfo {
	return nil
}

func (m *MockRdsHostListProviderService) SetHostListProvider(_ driver_infrastructure.HostListProvider) {
}
func (m *MockRdsHostListProviderService) SetInitialConnectionHostInfo(_ *host_info_util.HostInfo) {
}

type MockIamTokenUtility struct {
	CapturedUsername                       string
	CapturedHost                           string
	CapturedPort                           int
	CapturedRegion                         region_util.Region
	GenerateAuthenticationTokenCallCounter int
	GenerateTokenError                     error
}

func (m *MockIamTokenUtility) GenerateAuthenticationToken(
	username string,
	host string,
	port int,
	region region_util.Region,
	_ aws.CredentialsProvider,
	_ driver_infrastructure.PluginService,
) (string, error) {
	m.GenerateAuthenticationTokenCallCounter++
	m.CapturedUsername = username
	m.CapturedHost = host
	m.CapturedPort = port
	m.CapturedRegion = region
	if m.GenerateTokenError != nil {
		return "", m.GenerateTokenError
	}
	return m.GetMockTokenValue(), nil
}

func (m *MockIamTokenUtility) GetMockTokenValue() string {
	return "someToken"
}

func (m *MockIamTokenUtility) Reset() {
	m.CapturedUsername = ""
	m.CapturedHost = ""
	m.CapturedPort = 0
	m.CapturedRegion = ""
	m.GenerateAuthenticationTokenCallCounter = 0
	m.GenerateTokenError = nil
}

type MockHttpClient struct {
	doReturnValues []*http.Response
	doCallCount    *int
	getReturnValue *http.Response
	errReturnValue error
}

func (m MockHttpClient) Get(_ string) (*http.Response, error) {
	if m.getReturnValue != nil {
		return m.getReturnValue, m.errReturnValue
	}
	return nil, nil
}

func (m MockHttpClient) Do(_ *http.Request) (*http.Response, error) {
	if m.doCallCount == nil {
		if len(m.doReturnValues) > 0 {
			return m.doReturnValues[0], m.errReturnValue
		}
		return nil, m.errReturnValue
	}

	idx := *m.doCallCount
	maxIndex := len(m.doReturnValues) - 1
	if idx > maxIndex {
		idx = maxIndex
	}

	resp := m.doReturnValues[idx]

	*m.doCallCount++
	return resp, m.errReturnValue
}

type MockCredentialsProviderFactory struct {
	getAwsCredentialsProviderError error
}

func (m MockCredentialsProviderFactory) GetAwsCredentialsProvider(_ string, _ region_util.Region, _ *utils.RWMap[string, string]) (aws.CredentialsProvider, error) {
	if m.getAwsCredentialsProviderError != nil {
		return nil, m.getAwsCredentialsProviderError
	}
	return nil, nil
}

type MockAwsStsClient struct {
	assumeRoleWithSAMLErr         error
	assumeRoleWithSAMLReturnValue *sts.AssumeRoleWithSAMLOutput
}

func (m MockAwsStsClient) AssumeRoleWithSAML(_ context.Context, _ *sts.AssumeRoleWithSAMLInput, _ ...func(*sts.Options)) (*sts.AssumeRoleWithSAMLOutput, error) {
	if m.assumeRoleWithSAMLErr != nil {
		return nil, m.assumeRoleWithSAMLErr
	}
	if m.assumeRoleWithSAMLReturnValue != nil {
		return m.assumeRoleWithSAMLReturnValue, nil
	}
	return nil, nil
}

func NewMockAwsStsClient(_ string) auth_helpers.AwsStsClient {
	return &MockAwsStsClient{}
}

// --- Aws Services Mocks. ---

// Secrets Manager Mocks.
type MockAwsSecretsManagerClient struct {
}

func (m *MockAwsSecretsManagerClient) GetSecretValue(_ context.Context,
	_ *secretsmanager.GetSecretValueInput,
	_ ...func(*secretsmanager.Options),
) (*secretsmanager.GetSecretValueOutput, error) {
	mockOutput := secretsmanager.GetSecretValueOutput{
		ARN:          aws.String("arn:aws:secretsmanager:us-west-2:account-id:secret:default"),
		Name:         aws.String("default-secret-name"),
		SecretString: aws.String("{\"username\":\"testuser\",\"password\":\"testpassword\"}"),
		VersionId:    aws.String("default-version-id"),
	}
	return &mockOutput, nil
}

func NewMockAwsSecretsManagerClient(_ *host_info_util.HostInfo,
	_ *utils.RWMap[string, string],
	_ string,
	_ string) (aws_secrets_manager.AwsSecretsManagerClient, error) {
	return &MockAwsSecretsManagerClient{}, nil
}

type MockConnectionProvider struct{}

func (m *MockConnectionProvider) AcceptsUrl(_ host_info_util.HostInfo, _ map[string]string) bool {
	return false
}

func (m *MockConnectionProvider) AcceptsStrategy(_ string) bool {
	return false
}

func (m *MockConnectionProvider) GetHostSelectorStrategy(_ string) (driver_infrastructure.HostSelector, error) {
	return nil, nil
}

func (m *MockConnectionProvider) GetHostInfoByStrategy(
	_ []*host_info_util.HostInfo,
	_ host_info_util.HostRole,
	_ string,
	_ map[string]string) (*host_info_util.HostInfo, error) {
	return nil, nil
}

func (m *MockConnectionProvider) Connect(_ *host_info_util.HostInfo, _ map[string]string, _ driver_infrastructure.PluginService) (driver.Conn, error) {
	return &MockConn{}, nil
}
