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

	aws_secrets_manager "github.com/aws/aws-advanced-go-wrapper/aws-secrets-manager"
	"github.com/aws/aws-advanced-go-wrapper/awssql/internal_pool"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/limitless"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	mysql_driver "github.com/aws/aws-advanced-go-wrapper/mysql-driver"
	"github.com/aws/aws-advanced-go-wrapper/okta"
	"github.com/aws/aws-advanced-go-wrapper/otlp"
	pgx_driver "github.com/aws/aws-advanced-go-wrapper/pgx-driver"
	"github.com/aws/aws-advanced-go-wrapper/xray"

	"testing"

	awsDriver "github.com/aws/aws-advanced-go-wrapper/awssql/driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/bg"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/efm"
	federated_auth "github.com/aws/aws-advanced-go-wrapper/federated-auth"
	"github.com/aws/aws-advanced-go-wrapper/iam"
)

func TestImplementations(t *testing.T) {
	// Check for correct implementations of interfaces on left.

	var _ driver_infrastructure.DatabaseDialect = (*driver_infrastructure.RdsMultiAzClusterMySQLDatabaseDialect)(nil)
	var _ driver_infrastructure.DatabaseDialect = (*driver_infrastructure.RdsMultiAzClusterPgDatabaseDialect)(nil)
	var _ driver_infrastructure.DatabaseDialect = (*driver_infrastructure.MySQLDatabaseDialect)(nil)
	var _ driver_infrastructure.DatabaseDialect = (*driver_infrastructure.RdsMySQLDatabaseDialect)(nil)
	var _ driver_infrastructure.DatabaseDialect = (*driver_infrastructure.AuroraMySQLDatabaseDialect)(nil)
	var _ driver_infrastructure.DatabaseDialect = (*driver_infrastructure.PgDatabaseDialect)(nil)
	var _ driver_infrastructure.DatabaseDialect = (*driver_infrastructure.RdsPgDatabaseDialect)(nil)
	var _ driver_infrastructure.DatabaseDialect = (*driver_infrastructure.AuroraPgDatabaseDialect)(nil)
	var _ driver_infrastructure.BlueGreenDialect = (*driver_infrastructure.RdsMySQLDatabaseDialect)(nil)
	var _ driver_infrastructure.BlueGreenDialect = (*driver_infrastructure.AuroraMySQLDatabaseDialect)(nil)
	var _ driver_infrastructure.BlueGreenDialect = (*driver_infrastructure.RdsPgDatabaseDialect)(nil)
	var _ driver_infrastructure.BlueGreenDialect = (*driver_infrastructure.AuroraPgDatabaseDialect)(nil)
	var _ driver_infrastructure.TopologyAwareDialect = (*driver_infrastructure.MySQLTopologyAwareDatabaseDialect)(nil)
	var _ driver_infrastructure.TopologyAwareDialect = (*driver_infrastructure.PgTopologyAwareDatabaseDialect)(nil)
	var _ driver_infrastructure.TopologyAwareDialect = (*driver_infrastructure.AuroraMySQLDatabaseDialect)(nil)
	var _ driver_infrastructure.TopologyAwareDialect = (*driver_infrastructure.AuroraPgDatabaseDialect)(nil)
	var _ driver_infrastructure.TopologyAwareDialect = (*driver_infrastructure.RdsMultiAzClusterPgDatabaseDialect)(nil)
	var _ driver_infrastructure.TopologyAwareDialect = (*driver_infrastructure.RdsMultiAzClusterMySQLDatabaseDialect)(nil)
	var _ driver_infrastructure.DialectProvider = (*driver_infrastructure.DialectManager)(nil)
	var _ driver_infrastructure.PluginManager = (*plugin_helpers.PluginManagerImpl)(nil)
	var _ driver_infrastructure.PluginService = (*plugin_helpers.PluginServiceImpl)(nil)
	var _ driver_infrastructure.CanReleaseResources = (*plugin_helpers.PluginManagerImpl)(nil)
	var _ driver_infrastructure.CanReleaseResources = (*plugin_helpers.PluginServiceImpl)(nil)
	var _ driver_infrastructure.HostListProviderService = (*plugin_helpers.PluginServiceImpl)(nil)
	var _ driver_infrastructure.HostSelector = (*driver_infrastructure.RandomHostSelector)(nil)
	var _ driver_infrastructure.HostSelector = (*driver_infrastructure.WeightedRandomHostSelector)(nil)
	var _ driver_infrastructure.HostSelector = (*driver_infrastructure.RoundRobinHostSelector)(nil)
	var _ driver_infrastructure.WeightedHostSelector = (*driver_infrastructure.WeightedRandomHostSelector)(nil)
	var _ driver_infrastructure.HostListProvider = (*driver_infrastructure.DsnHostListProvider)(nil)
	var _ driver_infrastructure.HostListProvider = (*driver_infrastructure.RdsHostListProvider)(nil)
	var _ driver_infrastructure.HostListProvider = (*driver_infrastructure.MonitoringRdsHostListProvider)(nil)
	var _ driver_infrastructure.BlockingHostListProvider = (*driver_infrastructure.MonitoringRdsHostListProvider)(nil)
	var _ driver_infrastructure.ClusterTopologyMonitor = (*driver_infrastructure.ClusterTopologyMonitorImpl)(nil)
	var _ driver_infrastructure.ConnectionProvider = (*driver_infrastructure.DriverConnectionProvider)(nil)
	var _ driver_infrastructure.ConnectionProvider = (*internal_pool.InternalPooledConnectionProvider)(nil)
	var _ driver_infrastructure.ConnectionPlugin = (*plugins.DefaultPlugin)(nil)
	var _ driver_infrastructure.ConnectionPlugin = (*plugins.BaseConnectionPlugin)(nil)
	var _ driver_infrastructure.ConnectionPlugin = (*efm.HostMonitorConnectionPlugin)(nil)
	var _ driver_infrastructure.ConnectionPlugin = (*plugins.FailoverPlugin)(nil)
	var _ driver_infrastructure.ConnectionPlugin = (*iam.IamAuthPlugin)(nil)
	var _ driver_infrastructure.ConnectionPlugin = (*aws_secrets_manager.AwsSecretsManagerPlugin)(nil)
	var _ driver_infrastructure.ConnectionPlugin = (*okta.OktaAuthPlugin)(nil)
	var _ driver_infrastructure.ConnectionPlugin = (*federated_auth.FederatedAuthPlugin)(nil)
	var _ driver_infrastructure.ConnectionPlugin = (*bg.BlueGreenPlugin)(nil)
	var _ driver_infrastructure.ConnectionPlugin = (*plugins.ConnectTimePlugin)(nil)
	var _ driver_infrastructure.ConnectionPlugin = (*plugins.ExecutionTimePlugin)(nil)
	var _ driver_infrastructure.ConnectionPluginFactory = (*plugins.ConnectTimePluginFactory)(nil)
	var _ driver_infrastructure.ConnectionPluginFactory = (*plugins.ExecutionTimePluginFactory)(nil)
	var _ driver_infrastructure.ConnectionPluginFactory = (*efm.HostMonitoringPluginFactory)(nil)
	var _ driver_infrastructure.ConnectionPluginFactory = (*plugins.FailoverPluginFactory)(nil)
	var _ driver_infrastructure.ConnectionPluginFactory = (*bg.BlueGreenPluginFactory)(nil)
	var _ driver_infrastructure.ConnectionPluginFactory = (*iam.IamAuthPluginFactory)(nil)
	var _ driver_infrastructure.ConnectionPluginFactory = (*aws_secrets_manager.AwsSecretsManagerPluginFactory)(nil)
	var _ driver_infrastructure.ConnectionPluginFactory = (*okta.OktaAuthPluginFactory)(nil)
	var _ driver_infrastructure.ConnectionPluginFactory = (*federated_auth.FederatedAuthPluginFactory)(nil)
	var _ driver_infrastructure.ConnectionPluginFactory = (*limitless.LimitlessPluginFactory)(nil)
	var _ telemetry.TelemetryCounter = (*telemetry.NilTelemetryCounter)(nil)
	var _ telemetry.TelemetryCounter = (*otlp.OpenTelemetryCounter)(nil)
	var _ telemetry.TelemetryContext = (*telemetry.NilTelemetryContext)(nil)
	var _ telemetry.TelemetryContext = (*otlp.OpenTelemetryContext)(nil)
	var _ telemetry.TelemetryContext = (*xray.XRayTelemetryContext)(nil)
	var _ telemetry.TelemetryFactory = (*telemetry.NilTelemetryFactory)(nil)
	var _ telemetry.TelemetryFactory = (*otlp.OpenTelemetryFactory)(nil)
	var _ telemetry.TelemetryFactory = (*xray.XRayTelemetryFactory)(nil)
	var _ telemetry.TelemetryFactory = (*telemetry.DefaultTelemetryFactory)(nil)
	var _ efm.MonitorService = (*efm.MonitorServiceImpl)(nil)
	var _ efm.Monitor = (*efm.MonitorImpl)(nil)
	var _ error_util.ErrorHandler = (*mysql_driver.MySQLErrorHandler)(nil)
	var _ error_util.ErrorHandler = (*mysql_driver.MySQLDriverDialect)(nil)
	var _ error_util.ErrorHandler = (*pgx_driver.PgxErrorHandler)(nil)
	var _ error_util.ErrorHandler = (*pgx_driver.PgxDriverDialect)(nil)
	var _ error_util.ErrorHandler = (*plugin_helpers.PluginServiceImpl)(nil)
	var _ driver.Driver = (*awsDriver.AwsWrapperDriver)(nil)
	var _ driver.Driver = (*mysql_driver.MySQLDriver)(nil)
	var _ driver.Driver = (*pgx_driver.PgxDriver)(nil)
	var _ driver.Conn = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.Pinger = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.ExecerContext = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.QueryerContext = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.ConnPrepareContext = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.ConnBeginTx = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.SessionResetter = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.Validator = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.NamedValueChecker = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.Stmt = (*awsDriver.AwsWrapperStmt)(nil)
	var _ driver.StmtExecContext = (*awsDriver.AwsWrapperStmt)(nil)
	var _ driver.StmtQueryContext = (*awsDriver.AwsWrapperStmt)(nil)
	var _ driver.NamedValueChecker = (*awsDriver.AwsWrapperStmt)(nil)
	var _ driver.Result = (*awsDriver.AwsWrapperResult)(nil)
	var _ driver.Tx = (*awsDriver.AwsWrapperTx)(nil)
	var _ driver.Rows = (*awsDriver.AwsWrapperRows)(nil)
	var _ driver.RowsColumnTypeDatabaseTypeName = (*awsDriver.AwsWrapperRows)(nil)
	var _ driver.RowsColumnTypePrecisionScale = (*awsDriver.AwsWrapperRows)(nil)
	var _ driver.RowsColumnTypeLength = (*awsDriver.AwsWrapperPgRows)(nil)
	var _ driver.RowsNextResultSet = (*awsDriver.AwsWrapperMySQLRows)(nil)
	var _ driver.RowsColumnTypeScanType = (*awsDriver.AwsWrapperMySQLRows)(nil)
	var _ driver.RowsColumnTypeNullable = (*awsDriver.AwsWrapperMySQLRows)(nil)
	var _ error = (*error_util.AwsWrapperError)(nil)
	var _ driver_infrastructure.ExecuteRouting = (*bg.SuspendExecuteRouting)(nil)
	var _ driver_infrastructure.ConnectRouting = (*bg.SuspendConnectRouting)(nil)
	var _ driver_infrastructure.ConnectRouting = (*bg.SubstituteConnectRouting)(nil)
	var _ driver_infrastructure.ConnectRouting = (*bg.RejectConnectRouting)(nil)
	var _ driver_infrastructure.ConnectRouting = (*bg.SuspendUntilCorrespondingHostFoundConnectRouting)(nil)
}
