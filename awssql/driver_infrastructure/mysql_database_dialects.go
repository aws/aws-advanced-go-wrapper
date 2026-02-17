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

package driver_infrastructure

import (
	"context"
	"database/sql/driver"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

type MySQLDatabaseDialect struct {
}

func (m *MySQLDatabaseDialect) GetDialectUpdateCandidates() []string {
	return []string{RDS_MYSQL_MULTI_AZ_CLUSTER_DIALECT, AURORA_MYSQL_DIALECT, RDS_MYSQL_DIALECT}
}

func (m *MySQLDatabaseDialect) GetDefaultPort() int {
	return 3306
}

func (m *MySQLDatabaseDialect) GetHostAliasQuery() string {
	return "SELECT CONCAT(@@hostname, ':', @@port)"
}

func (m *MySQLDatabaseDialect) GetServerVersionQuery() string {
	return "SHOW VARIABLES LIKE 'version_comment'"
}

func (m *MySQLDatabaseDialect) IsDialect(conn driver.Conn) bool {
	// MysqlDialect and RdsMysqlDialect use the same server version query to determine the dialect.
	// The `SHOW VARIABLES LIKE 'version_comment'` outputs
	// | Variable_name   | value                        |
	// |-----------------|------------------------------|
	// | version_comment | MySQL Community Server (GPL) |
	// for community Mysql.
	row := utils.GetFirstRowFromQueryAsString(conn, m.GetServerVersionQuery())
	if len(row) > 1 && strings.Contains(row[1], "MySQL") {
		return true
	}
	return false
}

func (m *MySQLDatabaseDialect) GetHostListProvider(
	props *utils.RWMap[string, string],
	hostListProviderService HostListProviderService,
	_ PluginService) HostListProvider {
	return NewDsnHostListProvider(props, hostListProviderService)
}

func (m *MySQLDatabaseDialect) GetSetAutoCommitQuery(autoCommit bool) (string, error) {
	return fmt.Sprintf("set autocommit=%v", autoCommit), nil
}

func (m *MySQLDatabaseDialect) GetSetReadOnlyQuery(readOnly bool) (string, error) {
	readOnlyStr := "only"
	if !readOnly {
		readOnlyStr = "write"
	}
	return fmt.Sprintf("set session transaction read %v", readOnlyStr), nil
}

func (m *MySQLDatabaseDialect) GetSetCatalogQuery(catalog string) (string, error) {
	return fmt.Sprintf("use %v", catalog), nil
}

func (m *MySQLDatabaseDialect) GetSetSchemaQuery(_ string) (string, error) {
	return "", error_util.NewGenericAwsWrapperError(error_util.GetMessage("AwsWrapper.unsupportedMethodError", "SetSchema", fmt.Sprintf("%T", m)))
}

func (m *MySQLDatabaseDialect) GetSetTransactionIsolationQuery(level TransactionIsolationLevel) (string, error) {
	levelStr := ""
	switch level {
	case TRANSACTION_READ_UNCOMMITTED:
		levelStr = "READ UNCOMMITTED"
	case TRANSACTION_READ_COMMITTED:
		levelStr = "READ COMMITTED"
	case TRANSACTION_REPEATABLE_READ:
		levelStr = "REPEATABLE READ"
	case TRANSACTION_SERIALIZABLE:
		levelStr = "SERIALIZABLE"
	default:
		return "", error_util.NewGenericAwsWrapperError(error_util.GetMessage("DatabaseDialect.invalidTransactionIsolationLevel", levelStr))
	}
	return fmt.Sprintf("set session transaction isolation level %v", levelStr), nil
}

func (m *MySQLDatabaseDialect) DoesStatementSetAutoCommit(statement string) (bool, bool) {
	lowercaseStatement := strings.ToLower(statement)
	if strings.HasPrefix(lowercaseStatement, "set autocommit") {
		sections := strings.Split(lowercaseStatement, "=")
		if len(sections) < 2 {
			return false, false
		}
		result, err := strconv.ParseBool(strings.TrimSpace(sections[1]))
		if err != nil {
			return false, false
		}
		return result, true
	}

	return false, false
}

func (m *MySQLDatabaseDialect) DoesStatementSetCatalog(statement string) (string, bool) {
	re := regexp.MustCompile(`^(?i)use\s+(\w+)`)
	matches := re.FindStringSubmatch(statement)
	if len(matches) < 2 {
		return "", false
	}
	return matches[1], true
}

func (m *MySQLDatabaseDialect) DoesStatementSetReadOnly(statement string) (bool, bool) {
	lowercaseStatement := strings.ToLower(statement)
	if strings.HasPrefix(lowercaseStatement, "set session transaction read only") {
		return true, true
	}

	if strings.HasPrefix(lowercaseStatement, "set session transaction read write") {
		return false, true
	}

	return false, false
}

func (m *MySQLDatabaseDialect) DoesStatementSetSchema(_ string) (string, bool) {
	return "", false
}

func (m *MySQLDatabaseDialect) DoesStatementSetTransactionIsolation(statement string) (TransactionIsolationLevel, bool) {
	lowercaseStatement := strings.ToLower(statement)
	if strings.Contains(lowercaseStatement, "set session transaction isolation level read uncommitted") {
		return TRANSACTION_READ_UNCOMMITTED, true
	}
	if strings.Contains(lowercaseStatement, "set session transaction isolation level read committed") {
		return TRANSACTION_READ_COMMITTED, true
	}
	if strings.Contains(lowercaseStatement, "set session transaction isolation level repeatable read") {
		return TRANSACTION_REPEATABLE_READ, true
	}
	if strings.Contains(lowercaseStatement, "set session transaction isolation level serializable") {
		return TRANSACTION_SERIALIZABLE, true
	}

	return TRANSACTION_READ_UNCOMMITTED, false
}

func (m *MySQLDatabaseDialect) GetIsReaderQuery() string {
	return "SELECT @@read_only"
}

type RdsMySQLDatabaseDialect struct {
	MySQLDatabaseDialect
}

func (m *RdsMySQLDatabaseDialect) GetDialectUpdateCandidates() []string {
	return []string{RDS_MYSQL_MULTI_AZ_CLUSTER_DIALECT, AURORA_MYSQL_DIALECT}
}

func (m *RdsMySQLDatabaseDialect) IsDialect(conn driver.Conn) bool {
	// MySQLDatabaseDialect and RdsMySQLDatabaseDialect use the same server version query to determine the dialect.
	// The `SHOW VARIABLES LIKE 'version_comment'` outputs
	// | Variable_name   | value               |
	// |-----------------|---------------------|
	// | version_comment | Source distribution |
	// for RDS MySQL.
	row := utils.GetFirstRowFromQueryAsString(conn, m.GetServerVersionQuery())
	if len(row) > 1 && strings.Contains(row[1], "Source distribution") {
		return true
	}
	return false
}

func (m *RdsMySQLDatabaseDialect) GetBlueGreenStatus(conn driver.Conn) []BlueGreenResult {
	bgStatusQuery := "SELECT version, endpoint, port, role, status FROM mysql.rds_topology"
	return mySqlGetBlueGreenStatus(conn, bgStatusQuery)
}

func (m *RdsMySQLDatabaseDialect) IsBlueGreenStatusAvailable(conn driver.Conn) bool {
	topologyTableExistQuery := "SELECT 1 AS tmp FROM information_schema.tables WHERE table_schema = 'mysql' AND table_name = 'rds_topology'"
	return utils.GetFirstRowFromQuery(conn, topologyTableExistQuery) != nil
}

type AuroraMySQLDatabaseDialect struct {
	MySQLDatabaseDialect
}

func (m *AuroraMySQLDatabaseDialect) GetDialectUpdateCandidates() []string {
	return []string{RDS_MYSQL_MULTI_AZ_CLUSTER_DIALECT}
}

func (m *AuroraMySQLDatabaseDialect) IsDialect(conn driver.Conn) bool {
	row := utils.GetFirstRowFromQueryAsString(conn, "SHOW VARIABLES LIKE 'aurora_version'")
	// If a variable with such name is presented then it means it's an Aurora cluster.
	return row != nil
}

func (m *AuroraMySQLDatabaseDialect) GetTopologyQuery() string {
	return "SELECT @@aurora_server_id;"
}

func (m *AuroraMySQLDatabaseDialect) GetInstanceIdQuery() string {
	return "SELECT server_id, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END as is_writer, " +
		"cpu, REPLICA_LAG_IN_MILLISECONDS as 'lag', LAST_UPDATE_TIMESTAMP as last_update_timestamp " +
		"FROM information_schema.replica_host_status " +
		// Filter out hosts that haven't been updated in the last 5 minutes.
		"WHERE time_to_sec(timediff(now(), LAST_UPDATE_TIMESTAMP)) <= 300 OR SESSION_ID = 'MASTER_SESSION_ID' "
}

func (m *AuroraMySQLDatabaseDialect) GetWriterIdQuery() string {
	return "SELECT server_id " +
		"FROM information_schema.replica_host_status " +
		"WHERE SESSION_ID = 'MASTER_SESSION_ID' AND SERVER_ID = @@aurora_server_id"
}

func (m *AuroraMySQLDatabaseDialect) GetHostListProvider(
	props *utils.RWMap[string, string],
	hostListProviderService HostListProviderService,
	pluginService PluginService) HostListProvider {
	topologyUtils := NewAuroraTopologyUtils(m, MySQLRowParser)
	return getMySQLTopologyAwareHostListProvider(m, topologyUtils, props, hostListProviderService, pluginService)
}

func (m *AuroraMySQLDatabaseDialect) GetBlueGreenStatus(conn driver.Conn) []BlueGreenResult {
	bgStatusQuery := "SELECT version, endpoint, port, role, status FROM mysql.rds_topology"
	return mySqlGetBlueGreenStatus(conn, bgStatusQuery)
}

func (m *AuroraMySQLDatabaseDialect) IsBlueGreenStatusAvailable(conn driver.Conn) bool {
	topologyTableExistQuery := "SELECT 1 AS tmp FROM information_schema.tables WHERE table_schema = 'mysql' AND table_name = 'rds_topology'"
	return utils.GetFirstRowFromQuery(conn, topologyTableExistQuery) != nil
}

type RdsMultiAzClusterMySQLDatabaseDialect struct {
	MySQLDatabaseDialect
}

func (r *RdsMultiAzClusterMySQLDatabaseDialect) IsDialect(conn driver.Conn) bool {
	// We need to check that
	// 1. rds_topology table exists
	// 2. we can query the topology table
	// 3. That it reports the host variable and gets the ip address

	topologyTableExistQuery := "SELECT 1 AS tmp FROM information_schema.tables WHERE table_schema = 'mysql' AND table_name = 'rds_topology'"
	topologyQuery := "SELECT id, endpoint, port FROM mysql.rds_topology"
	reportHostQuery := "SHOW VARIABLES LIKE 'report_host'"

	// Verify topology table exists
	row := utils.GetFirstRowFromQueryAsString(conn, topologyTableExistQuery)
	if row == nil {
		return false
	}

	// Verify that topology table is not empty
	row = utils.GetFirstRowFromQueryAsString(conn, topologyQuery)
	if row == nil {
		return false
	}

	// Verify that report host variable gets ip address
	row = utils.GetFirstRowFromQueryAsString(conn, reportHostQuery)
	if len(row) > 1 {
		return row[1] != ""
	}

	return false
}

func (r *RdsMultiAzClusterMySQLDatabaseDialect) GetDialectUpdateCandidates() []string {
	return nil
}

func (r *RdsMultiAzClusterMySQLDatabaseDialect) GetTopologyQuery() string {
	return "SELECT id, endpoint FROM mysql.rds_topology"
}

func (r *RdsMultiAzClusterMySQLDatabaseDialect) GetInstanceIdQuery() string {
	return "SELECT id, SUBSTRING_INDEX(endpoint, '.', 1)" +
		" FROM mysql.rds_topology" +
		" WHERE id = @@server_id"
}

func (r *RdsMultiAzClusterMySQLDatabaseDialect) GetWriterIdQuery() string {
	return "SHOW REPLICA STATUS"
}

func (r *RdsMultiAzClusterMySQLDatabaseDialect) GetWriterIdColumnName() string {
	return "Source_Server_Id"
}

func (r *RdsMultiAzClusterMySQLDatabaseDialect) GetHostListProvider(
	props *utils.RWMap[string, string],
	hostListProviderService HostListProviderService,
	pluginService PluginService) HostListProvider {
	topologyUtils := NewMultiAzTopologyUtils(r, MySQLRowParser)
	return getMySQLTopologyAwareHostListProvider(r, topologyUtils, props, hostListProviderService, pluginService)
}

func mySqlGetBlueGreenStatus(conn driver.Conn, query string) []BlueGreenResult {
	return getBlueGreenStatus(conn, query, utils.MySqlConvertValToString)
}

func getBlueGreenStatus(conn driver.Conn, query string, convertFunc func(driver.Value) (string, bool)) []BlueGreenResult {
	queryerCtx, ok := conn.(driver.QueryerContext)
	if !ok {
		// Unable to query, conn does not implement QueryerContext.
		slog.Warn(error_util.GetMessage("Conn.doesNotImplementRequiredInterface", "driver.QueryerContext"))
		return nil
	}

	rows, err := queryerCtx.QueryContext(context.Background(), query, nil)
	if err != nil {
		// Query failed.
		slog.Warn(error_util.GetMessage("BlueGreenDeployment.errorQueryingStatusTable", err))
		return nil
	}
	if rows != nil {
		defer rows.Close()
	}

	var statuses []BlueGreenResult
	row := make([]driver.Value, len(rows.Columns()))
	for rows.Next(row) == nil {
		if len(row) > 4 {
			version, ok1 := convertFunc(row[0])
			endpoint, ok2 := convertFunc(row[1])
			portAsFloat, ok3 := row[2].(int64)
			role, ok4 := convertFunc(row[3])
			status, ok5 := convertFunc(row[4])

			if !ok1 || !ok2 || !ok3 || !ok4 || !ok5 {
				continue
			}
			statuses = append(statuses, BlueGreenResult{
				Version:  version,
				Endpoint: endpoint,
				Port:     int(portAsFloat),
				Role:     role,
				Status:   status,
			})
		}
	}

	return statuses
}

func getMySQLTopologyAwareHostListProvider(
	dialect TopologyDialect,
	topologyUtil TopologyUtils,
	props *utils.RWMap[string, string],
	hostListProviderService HostListProviderService,
	pluginService PluginService) HostListProvider {
	pluginsProp := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.PLUGINS)

	if strings.Contains(pluginsProp, "failover") {
		slog.Debug(error_util.GetMessage("DatabaseDialect.usingMonitoringHostListProvider"))
		return NewMonitoringRdsHostListProvider(hostListProviderService, dialect, topologyUtil, props, pluginService)
	}

	slog.Debug(error_util.GetMessage("DatabaseDialect.usingRdsHostListProvider"))
	return NewRdsHostListProvider(hostListProviderService, dialect, topologyUtil, props, nil, nil)
}
