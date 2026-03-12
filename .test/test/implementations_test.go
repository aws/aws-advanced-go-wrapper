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
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/connection_tracker"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/limitless"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/read_write_splitting"
	"github.com/aws/aws-advanced-go-wrapper/awssql/services"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	custom_endpoint "github.com/aws/aws-advanced-go-wrapper/custom-endpoint"
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

	// =========================================================================
	// Database Dialects
	// =========================================================================

	// DatabaseDialect implementations
	var _ driver_infrastructure.DatabaseDialect = (*driver_infrastructure.MySQLDatabaseDialect)(nil)
	var _ driver_infrastructure.DatabaseDialect = (*driver_infrastructure.RdsMySQLDatabaseDialect)(nil)
	var _ driver_infrastructure.DatabaseDialect = (*driver_infrastructure.AuroraMySQLDatabaseDialect)(nil)
	var _ driver_infrastructure.DatabaseDialect = (*driver_infrastructure.RdsMultiAzClusterMySQLDatabaseDialect)(nil)
	var _ driver_infrastructure.DatabaseDialect = (*driver_infrastructure.PgDatabaseDialect)(nil)
	var _ driver_infrastructure.DatabaseDialect = (*driver_infrastructure.RdsPgDatabaseDialect)(nil)
	var _ driver_infrastructure.DatabaseDialect = (*driver_infrastructure.AuroraPgDatabaseDialect)(nil)
	var _ driver_infrastructure.DatabaseDialect = (*driver_infrastructure.RdsMultiAzClusterPgDatabaseDialect)(nil)

	// TopologyDialect implementations
	var _ driver_infrastructure.TopologyDialect = (*driver_infrastructure.AuroraMySQLDatabaseDialect)(nil)
	var _ driver_infrastructure.TopologyDialect = (*driver_infrastructure.AuroraPgDatabaseDialect)(nil)
	var _ driver_infrastructure.TopologyDialect = (*driver_infrastructure.RdsMultiAzClusterMySQLDatabaseDialect)(nil)
	var _ driver_infrastructure.TopologyDialect = (*driver_infrastructure.RdsMultiAzClusterPgDatabaseDialect)(nil)

	// MultiAzTopologyDialect implementations
	var _ driver_infrastructure.MultiAzTopologyDialect = (*driver_infrastructure.RdsMultiAzClusterMySQLDatabaseDialect)(nil)
	var _ driver_infrastructure.MultiAzTopologyDialect = (*driver_infrastructure.RdsMultiAzClusterPgDatabaseDialect)(nil)

	// AuroraLimitlessDialect implementations
	var _ driver_infrastructure.AuroraLimitlessDialect = (*driver_infrastructure.AuroraPgDatabaseDialect)(nil)

	// BlueGreenDialect implementations
	var _ driver_infrastructure.BlueGreenDialect = (*driver_infrastructure.RdsMySQLDatabaseDialect)(nil)
	var _ driver_infrastructure.BlueGreenDialect = (*driver_infrastructure.AuroraMySQLDatabaseDialect)(nil)
	var _ driver_infrastructure.BlueGreenDialect = (*driver_infrastructure.RdsPgDatabaseDialect)(nil)
	var _ driver_infrastructure.BlueGreenDialect = (*driver_infrastructure.AuroraPgDatabaseDialect)(nil)

	// RowParser implementations
	var _ = mysql_driver.MySQLDriverDialect{}.GetRowParser()
	var _ = pgx_driver.PgxDriverDialect{}.GetRowParser()

	// DialectProvider implementations
	var _ driver_infrastructure.DialectProvider = (*driver_infrastructure.DialectManager)(nil)

	// =========================================================================
	// Topology Utils
	// =========================================================================

	// TopologyUtils implementations
	var _ driver_infrastructure.TopologyUtils = (*driver_infrastructure.AuroraTopologyUtils)(nil)
	var _ driver_infrastructure.TopologyUtils = (*driver_infrastructure.MultiAzTopologyUtils)(nil)

	// =========================================================================
	// Services (Storage, Monitor, Event)
	// =========================================================================

	// StorageService implementations
	var _ driver_infrastructure.StorageService = (*services.ExpiringStorage)(nil)

	// RawStorageAccess implementations
	var _ driver_infrastructure.RawStorageAccess = (*services.ExpiringStorage)(nil)

	// MonitorService implementations
	var _ driver_infrastructure.MonitorService = (*services.MonitorManager)(nil)

	// EventPublisher implementations
	var _ driver_infrastructure.EventPublisher = (*services.BatchingEventPublisher)(nil)

	// Event implementations
	var _ driver_infrastructure.Event = services.DataAccessEvent{}
	var _ driver_infrastructure.Event = services.MonitorStopEvent{}
	var _ driver_infrastructure.Event = services.MonitorResetEvent{}

	// EventSubscriber implementations
	var _ driver_infrastructure.EventSubscriber = (*services.MonitorManager)(nil)
	var _ driver_infrastructure.EventSubscriber = (*driver_infrastructure.ClusterTopologyMonitorImpl)(nil)
	var _ driver_infrastructure.EventSubscriber = (*efm.HostMonitorImpl)(nil)

	// ServicesContainer implementations
	var _ driver_infrastructure.ServicesContainer = (*services.FullServicesContainer)(nil)

	// SessionStateService implementations
	var _ driver_infrastructure.SessionStateService = (*driver_infrastructure.SessionStateServiceImpl)(nil)

	// =========================================================================
	// Monitors (ClusterTopology, CustomEndpoint, EFM, Limitless)
	// =========================================================================

	// Monitor implementations
	var _ driver_infrastructure.Monitor = (*driver_infrastructure.ClusterTopologyMonitorImpl)(nil)
	var _ driver_infrastructure.Monitor = (*custom_endpoint.CustomEndpointMonitorImpl)(nil)
	var _ driver_infrastructure.Monitor = (*efm.HostMonitorImpl)(nil)
	var _ driver_infrastructure.Monitor = (*limitless.LimitlessRouterMonitorImpl)(nil)

	// ClusterTopologyMonitor implementations
	var _ driver_infrastructure.ClusterTopologyMonitor = (*driver_infrastructure.ClusterTopologyMonitorImpl)(nil)

	// CustomEndpointMonitor implementations
	var _ custom_endpoint.CustomEndpointMonitor = (*custom_endpoint.CustomEndpointMonitorImpl)(nil)

	// =========================================================================
	// Host List Providers
	// =========================================================================

	// HostListProvider implementations
	var _ driver_infrastructure.HostListProvider = (*driver_infrastructure.DsnHostListProvider)(nil)
	var _ driver_infrastructure.HostListProvider = (*driver_infrastructure.RdsHostListProvider)(nil)

	// BlockingHostListProvider implementations
	var _ driver_infrastructure.BlockingHostListProvider = (*driver_infrastructure.RdsHostListProvider)(nil)

	// =========================================================================
	// Host Selectors
	// =========================================================================

	var _ driver_infrastructure.HostSelector = (*driver_infrastructure.RandomHostSelector)(nil)
	var _ driver_infrastructure.HostSelector = (*driver_infrastructure.WeightedRandomHostSelector)(nil)
	var _ driver_infrastructure.HostSelector = (*driver_infrastructure.RoundRobinHostSelector)(nil)
	var _ driver_infrastructure.HostSelector = (*driver_infrastructure.HighestWeightHostSelector)(nil)
	var _ driver_infrastructure.WeightedHostSelector = (*driver_infrastructure.WeightedRandomHostSelector)(nil)

	// =========================================================================
	// Connection Providers
	// =========================================================================

	var _ driver_infrastructure.ConnectionProvider = (*driver_infrastructure.DriverConnectionProvider)(nil)
	var _ driver_infrastructure.ConnectionProvider = (*internal_pool.InternalPooledConnectionProvider)(nil)

	// =========================================================================
	// Plugin Manager and Plugin Service
	// =========================================================================

	var _ driver_infrastructure.PluginManager = (*plugin_helpers.PluginManagerImpl)(nil)
	var _ driver_infrastructure.PluginService = (*plugin_helpers.PluginServiceImpl)(nil)
	var _ driver_infrastructure.PluginService = (*plugin_helpers.PartialPluginService)(nil)
	var _ driver_infrastructure.CanReleaseResources = (*plugin_helpers.PluginManagerImpl)(nil)
	var _ driver_infrastructure.CanReleaseResources = (*plugin_helpers.PluginServiceImpl)(nil)
	var _ driver_infrastructure.HostListProviderService = (*plugin_helpers.PluginServiceImpl)(nil)

	// =========================================================================
	// Connection Plugins
	// =========================================================================

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
	var _ driver_infrastructure.ConnectionPlugin = (*plugins.AuroraConnectionTrackerPlugin)(nil)
	var _ driver_infrastructure.ConnectionPlugin = (*plugins.DeveloperConnectionPlugin)(nil)
	var _ driver_infrastructure.ConnectionPlugin = (*read_write_splitting.ReadWriteSplittingPlugin)(nil)
	var _ driver_infrastructure.ConnectionPlugin = (*custom_endpoint.CustomEndpointPlugin)(nil)
	var _ driver_infrastructure.ConnectionPlugin = (*limitless.LimitlessPlugin)(nil)
	var _ driver_infrastructure.ConnectionPlugin = (*plugins.AuroraInitialConnectionStrategyPlugin)(nil)

	// =========================================================================
	// Connection Plugin Factories
	// =========================================================================

	var _ driver_infrastructure.ConnectionPluginFactory = (*plugins.AuroraConnectionTrackerPluginFactory)(nil)
	var _ driver_infrastructure.ConnectionPluginFactory = (*plugins.DeveloperConnectionPluginFactory)(nil)
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
	var _ driver_infrastructure.ConnectionPluginFactory = (*read_write_splitting.ReadWriteSplittingPluginFactory)(nil)
	var _ driver_infrastructure.ConnectionPluginFactory = (*custom_endpoint.CustomEndpointPluginFactory)(nil)
	var _ driver_infrastructure.ConnectionPluginFactory = (*plugins.AuroraInitialConnectionStrategyPluginFactory)(nil)

	// =========================================================================
	// Connection Tracker
	// =========================================================================

	var _ connection_tracker.ConnectionTracker = (*connection_tracker.OpenedConnectionTracker)(nil)

	// =========================================================================
	// Telemetry
	// =========================================================================

	var _ telemetry.TelemetryCounter = (*telemetry.NilTelemetryCounter)(nil)
	var _ telemetry.TelemetryCounter = (*otlp.OpenTelemetryCounter)(nil)
	var _ telemetry.TelemetryContext = (*telemetry.NilTelemetryContext)(nil)
	var _ telemetry.TelemetryContext = (*otlp.OpenTelemetryContext)(nil)
	var _ telemetry.TelemetryContext = (*xray.XRayTelemetryContext)(nil)
	var _ telemetry.TelemetryFactory = (*telemetry.NilTelemetryFactory)(nil)
	var _ telemetry.TelemetryFactory = (*otlp.OpenTelemetryFactory)(nil)
	var _ telemetry.TelemetryFactory = (*xray.XRayTelemetryFactory)(nil)
	var _ telemetry.TelemetryFactory = (*telemetry.DefaultTelemetryFactory)(nil)

	// =========================================================================
	// Error Handling
	// =========================================================================

	var _ error_util.ErrorHandler = (*mysql_driver.MySQLErrorHandler)(nil)
	var _ error_util.ErrorHandler = (*mysql_driver.MySQLDriverDialect)(nil)
	var _ error_util.ErrorHandler = (*pgx_driver.PgxErrorHandler)(nil)
	var _ error_util.ErrorHandler = (*pgx_driver.PgxDriverDialect)(nil)
	var _ error_util.ErrorHandler = (*plugin_helpers.PluginServiceImpl)(nil)
	var _ error = (*error_util.AwsWrapperError)(nil)

	// =========================================================================
	// Driver Dialects
	// =========================================================================

	var _ driver_infrastructure.DriverDialect = (*mysql_driver.MySQLDriverDialect)(nil)
	var _ driver_infrastructure.DriverDialect = (*pgx_driver.PgxDriverDialect)(nil)

	// =========================================================================
	// database/sql/driver Interfaces
	// =========================================================================

	// Driver implementations
	var _ driver.Driver = (*awsDriver.AwsWrapperDriver)(nil)
	var _ driver.Driver = (*mysql_driver.MySQLDriver)(nil)
	var _ driver.Driver = (*pgx_driver.PgxDriver)(nil)

	// Conn and related interfaces
	var _ driver.Conn = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.Pinger = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.ExecerContext = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.QueryerContext = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.ConnPrepareContext = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.ConnBeginTx = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.SessionResetter = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.Validator = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.NamedValueChecker = (*awsDriver.AwsWrapperConn)(nil)

	// Stmt interfaces
	var _ driver.Stmt = (*awsDriver.AwsWrapperStmt)(nil)
	var _ driver.StmtExecContext = (*awsDriver.AwsWrapperStmt)(nil)
	var _ driver.StmtQueryContext = (*awsDriver.AwsWrapperStmt)(nil)
	var _ driver.NamedValueChecker = (*awsDriver.AwsWrapperStmt)(nil)

	// Result and Tx interfaces
	var _ driver.Result = (*awsDriver.AwsWrapperResult)(nil)
	var _ driver.Tx = (*awsDriver.AwsWrapperTx)(nil)

	// Rows interfaces
	var _ driver.Rows = (*awsDriver.AwsWrapperRows)(nil)
	var _ driver.RowsColumnTypeDatabaseTypeName = (*awsDriver.AwsWrapperRows)(nil)
	var _ driver.RowsColumnTypePrecisionScale = (*awsDriver.AwsWrapperRows)(nil)
	var _ driver.RowsColumnTypeLength = (*awsDriver.AwsWrapperPgRows)(nil)
	var _ driver.RowsNextResultSet = (*awsDriver.AwsWrapperMySQLRows)(nil)
	var _ driver.RowsColumnTypeScanType = (*awsDriver.AwsWrapperMySQLRows)(nil)
	var _ driver.RowsColumnTypeNullable = (*awsDriver.AwsWrapperMySQLRows)(nil)

	// =========================================================================
	// Blue/Green Deployment Routing
	// =========================================================================

	var _ driver_infrastructure.ExecuteRouting = (*bg.SuspendExecuteRouting)(nil)
	var _ driver_infrastructure.ConnectRouting = (*bg.SuspendConnectRouting)(nil)
	var _ driver_infrastructure.ConnectRouting = (*bg.SubstituteConnectRouting)(nil)
	var _ driver_infrastructure.ConnectRouting = (*bg.RejectConnectRouting)(nil)
	var _ driver_infrastructure.ConnectRouting = (*bg.SuspendUntilCorrespondingHostFoundConnectRouting)(nil)
}
