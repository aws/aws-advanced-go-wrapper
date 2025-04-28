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
	"awssql/driver_infrastructure"
	"awssql/host_info_util"
	"awssql/plugin_helpers"
	"awssql/region_util"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

var mysqlTestDsn = "someUser:somePassword@tcp(mydatabase.cluster-xyz.us-east-2.rds.amazonaws.com:3306)/myDatabase?foo=bar&pop=snap"
var pgTestDsn = "postgres://someUser:somePassword@mydatabase.cluster-xyz.us-east-2.rds.amazonaws.com:5432/pgx_test?sslmode=disable&foo=bar"

type TestPlugin struct {
	calls      *[]string
	id         int
	connection driver.Conn
	error      error
	isBefore   bool
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

func (t TestPlugin) Execute(methodName string, executeFunc driver_infrastructure.ExecuteFunc, methodArgs ...any) (any, any, bool, error) {
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
	hostInfo *host_info_util.HostInfo,
	properties map[string]string,
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	*t.calls = append(*t.calls, fmt.Sprintf("%s%v:before connect", reflect.TypeOf(t), t.id))
	if t.isBefore && t.error != nil {
		return nil, t.error
	}
	if t.connection != nil {
		*t.calls = append(*t.calls, fmt.Sprintf("%s%v:connection", reflect.TypeOf(t), t.id))
		return t.connection, nil
	}
	conn, err := connectFunc()
	if !t.isBefore && t.error != nil {
		return nil, t.error
	}
	*t.calls = append(*t.calls, fmt.Sprintf("%s%v:after connect", reflect.TypeOf(t), t.id))
	return conn, err
}

func (t TestPlugin) ForceConnect(
	hostInfo *host_info_util.HostInfo,
	properties map[string]string,
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	*t.calls = append(*t.calls, fmt.Sprintf("%s%v:before forceConnect", reflect.TypeOf(t), t.id))
	if t.connection != nil {
		*t.calls = append(*t.calls, fmt.Sprintf("%s%v:forced connection", reflect.TypeOf(t), t.id))
		return t.connection, nil
	}
	conn, err := connectFunc()
	*t.calls = append(*t.calls, fmt.Sprintf("%s%v:after forceConnect", reflect.TypeOf(t), t.id))
	return conn, err
}

func (t TestPlugin) AcceptsStrategy(role host_info_util.HostRole, strategy string) bool {
	return false
}

func (t TestPlugin) GetHostInfoByStrategy(
	role host_info_util.HostRole,
	strategy string,
	hosts []*host_info_util.HostInfo) (*host_info_util.HostInfo, error) {
	*t.calls = append(*t.calls, fmt.Sprintf("%s%v:before GetHostInfoByStrategy", reflect.TypeOf(t), t.id))
	*t.calls = append(*t.calls, fmt.Sprintf("%s%v:after GetHostInfoByStrategy", reflect.TypeOf(t), t.id))
	return nil, nil
}

func (t TestPlugin) NotifyConnectionChanged(changes map[driver_infrastructure.HostChangeOptions]bool) driver_infrastructure.OldConnectionSuggestedAction {
	return driver_infrastructure.NO_OPINION
}

func (t TestPlugin) NotifyHostListChanged(changes map[string]map[driver_infrastructure.HostChangeOptions]bool) {
	// Do nothing
}

func (t TestPlugin) InitHostProvider(
	initialUrl string,
	props map[string]string,
	hostListProviderService driver_infrastructure.HostListProviderService,
	initHostProviderFunc func() error) error {
	// Do nothing
	return nil
}

type MockTargetDriver struct{}

func (m MockTargetDriver) Open(name string) (driver.Conn, error) {
	return nil, nil
}

type MockDriverConnection struct {
	id       int
	IsClosed bool
}

func (m *MockDriverConnection) Prepare(query string) (driver.Stmt, error) {
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

func (m *MockDriverConnection) IsValid() bool {
	return true
}

func CreateTestPlugin(calls *[]string, id int, connection driver.Conn, err error, isBefore bool) driver_infrastructure.ConnectionPlugin {
	if calls == nil {
		calls = &[]string{}
	}
	testPlugin := driver_infrastructure.ConnectionPlugin(&TestPlugin{calls: calls, id: id, connection: connection, error: err, isBefore: isBefore})
	return testPlugin
}

type MockHostListProvider struct{}

func (m *MockHostListProvider) CreateHost(hostName string, role host_info_util.HostRole, lag float64, cpu float64, lastUpdateTime time.Time) *host_info_util.HostInfo {
	return nil
}

func (m *MockHostListProvider) ForceRefresh(conn driver.Conn) ([]*host_info_util.HostInfo, error) {
	return nil, nil
}

func (m *MockHostListProvider) GetClusterId() string {
	return ""
}

func (m *MockHostListProvider) GetHostRole(conn driver.Conn) host_info_util.HostRole {
	return host_info_util.UNKNOWN
}

func (m *MockHostListProvider) IdentifyConnection(conn driver.Conn) (*host_info_util.HostInfo, error) {
	hostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("hostA").Build()
	return hostInfo, nil
}

func (m *MockHostListProvider) IsStaticHostListProvider() bool {
	return false
}

func (m *MockHostListProvider) Refresh(conn driver.Conn) ([]*host_info_util.HostInfo, error) {
	hostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("hostA").Build()
	return []*host_info_util.HostInfo{hostInfo}, nil
}

type MockPluginManager struct {
	*plugin_helpers.PluginManagerImpl
	Changes           map[string]map[driver_infrastructure.HostChangeOptions]bool
	ForceConnectProps map[string]string
}

func (pluginManager *MockPluginManager) NotifyHostListChanged(changes map[string]map[driver_infrastructure.HostChangeOptions]bool) {
	pluginManager.Changes = changes
}

func (pluginManager *MockPluginManager) ForceConnect(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	isInitialConnection bool) (driver.Conn, error) {
	pluginManager.ForceConnectProps = props
	return &MockDriverConnection{}, nil
}

type MockConn struct {
	queryResult   driver.Rows
	execResult    driver.Result
	beginResult   driver.Tx
	prepareResult driver.Stmt
	throwError    bool
	closeCounter  int
}

func (m *MockConn) Close() error {
	m.closeCounter++
	return nil
}

func (m *MockConn) Prepare(query string) (driver.Stmt, error) {
	return m.prepareResult, nil
}

func (m *MockConn) Begin() (driver.Tx, error) {
	return m.beginResult, nil
}

func (m *MockConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if m.queryResult != nil || !m.throwError {
		return m.queryResult, nil
	}
	return nil, errors.New("test error")
}

func (m *MockConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return m.execResult, nil
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
	return errors.New("MockStmt error")
}

func (a MockStmt) Exec(args []driver.Value) (driver.Result, error) {
	return MockResult{}, errors.New("MockStmt error")
}

func (a MockStmt) NumInput() int {
	return 1
}

func (a MockStmt) Query(args []driver.Value) (driver.Rows, error) {
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
}

func (a MockTx) Commit() error {
	return errors.New("MockTx error")
}

func (a MockTx) Rollback() error {
	return errors.New("MockTx error")
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
	if t.throwNextError == 1 {
		t.throwNextError = 0
	} else if t.throwNextError == 0 {
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

func (m *MockRdsHostListProviderService) CreateHostListProvider(props map[string]string, dsn string) driver_infrastructure.HostListProvider {
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

func (m *MockRdsHostListProviderService) SetHostListProvider(hostListProvider driver_infrastructure.HostListProvider) {
}
func (m *MockRdsHostListProviderService) SetInitialConnectionHostInfo(info *host_info_util.HostInfo) {
}

type MockIamTokenUtility struct {
	CapturedUsername                       string
	CapturedHost                           string
	CapturedPort                           int
	CapturedRegion                         region_util.Region
	GenerateAuthenticationTokenCallCounter int
}

func (m *MockIamTokenUtility) GenerateAuthenticationToken(
	username string,
	host string,
	port int,
	region region_util.Region,
	awsCredentialsProvider aws.CredentialsProvider,
) (string, error) {
	m.GenerateAuthenticationTokenCallCounter++
	m.CapturedUsername = username
	m.CapturedHost = host
	m.CapturedPort = port
	m.CapturedRegion = region
	return m.GetMockTokenValue(), nil
}

func (m *MockIamTokenUtility) GetMockTokenValue() string {
	return "someToken"
}

// --- Aws Services Mocks. ---

// Secrets Manager Mocks.
type MockAwsSecretsManagerClient struct {
}

func (m *MockAwsSecretsManagerClient) GetSecretValue(ctx context.Context,
	params *secretsmanager.GetSecretValueInput,
	optFns ...func(*secretsmanager.Options),
) (*secretsmanager.GetSecretValueOutput, error) {
	mockOutput := secretsmanager.GetSecretValueOutput{
		ARN:          aws.String("arn:aws:secretsmanager:us-west-2:account-id:secret:default"),
		Name:         aws.String("default-secret-name"),
		SecretString: aws.String("{\"username\":\"testuser\",\"password\":\"testpassword\"}"),
		VersionId:    aws.String("default-version-id"),
	}
	return &mockOutput, nil
}

func NewMockAwsSecretsManagerClient(hostInfo *host_info_util.HostInfo,
	props map[string]string,
	endpoint string,
	region string) (driver_infrastructure.AwsSecretsManagerClient, error) {
	client := MockAwsSecretsManagerClient{}

	return &client, nil
}
