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

package main

import (
	awsDriver "awssql/driver"
	"awssql/driver_infrastructure"
	"awssql/error_util"
	"awssql/plugin_helpers"
	"awssql/plugins"
	"database/sql/driver"
	"testing"
)

func TestImplementations(t *testing.T) {
	// Check for correct implementations of interfaces on left.
	var _ driver_infrastructure.DatabaseDialect = (*driver_infrastructure.MySQLDatabaseDialect)(nil)
	var _ driver_infrastructure.DatabaseDialect = (*driver_infrastructure.RdsMySQLDatabaseDialect)(nil)
	var _ driver_infrastructure.DatabaseDialect = (*driver_infrastructure.AuroraMySQLDatabaseDialect)(nil)
	var _ driver_infrastructure.DatabaseDialect = (*driver_infrastructure.PgDatabaseDialect)(nil)
	var _ driver_infrastructure.DatabaseDialect = (*driver_infrastructure.RdsPgDatabaseDialect)(nil)
	var _ driver_infrastructure.DatabaseDialect = (*driver_infrastructure.AuroraPgDatabaseDialect)(nil)
	var _ driver_infrastructure.TopologyAwareDialect = (*driver_infrastructure.AuroraMySQLDatabaseDialect)(nil)
	var _ driver_infrastructure.TopologyAwareDialect = (*driver_infrastructure.AuroraPgDatabaseDialect)(nil)
	var _ driver_infrastructure.DialectProvider = (*driver_infrastructure.DialectManager)(nil)
	var _ driver_infrastructure.PluginManager = (*plugin_helpers.PluginManagerImpl)(nil)
	var _ driver_infrastructure.PluginService = (*plugin_helpers.PluginServiceImpl)(nil)
	var _ driver_infrastructure.HostListProviderService = (*plugin_helpers.PluginServiceImpl)(nil)
	var _ driver_infrastructure.CanReleaseResources = (*plugin_helpers.PluginManagerImpl)(nil)
	var _ driver_infrastructure.CanReleaseResources = (*plugin_helpers.PluginServiceImpl)(nil)
	var _ driver_infrastructure.HostListProviderService = (*plugin_helpers.PluginServiceImpl)(nil)
	var _ driver_infrastructure.HostSelector = (*driver_infrastructure.RandomHostSelector)(nil)
	var _ driver_infrastructure.HostListProvider = (*driver_infrastructure.DsnHostListProvider)(nil)
	var _ driver_infrastructure.HostListProvider = (*driver_infrastructure.RdsHostListProvider)(nil)
	var _ driver_infrastructure.ConnectionProvider = (*driver_infrastructure.DriverConnectionProvider)(nil)
	var _ driver_infrastructure.ConnectionPlugin = (*plugins.DefaultPlugin)(nil)
	var _ error_util.ErrorHandler = (*driver_infrastructure.MySQLErrorHandler)(nil)
	var _ error_util.ErrorHandler = (*driver_infrastructure.MySQLDriverDialect)(nil)
	var _ error_util.ErrorHandler = (*driver_infrastructure.PgxErrorHandler)(nil)
	var _ error_util.ErrorHandler = (*driver_infrastructure.PgxDriverDialect)(nil)
	var _ error_util.ErrorHandler = (*plugin_helpers.PluginServiceImpl)(nil)
	var _ driver.Driver = (*awsDriver.AwsWrapperDriver)(nil)
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
}
