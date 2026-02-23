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
	"database/sql/driver"
	"fmt"
	"regexp"
	"strings"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_info"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

type PgDatabaseDialect struct {
}

func (p *PgDatabaseDialect) GetDialectUpdateCandidates() []string {
	return []string{RDS_PG_MULTI_AZ_CLUSTER_DIALECT, AURORA_PG_DIALECT, RDS_PG_DIALECT}
}

func (p *PgDatabaseDialect) GetDefaultPort() int {
	return 5432
}

func (p *PgDatabaseDialect) GetHostAliasQuery() string {
	return "SELECT pg_catalog.CONCAT(pg_catalog.inet_server_addr(), ':', pg_catalog.inet_server_port())"
}

func (p *PgDatabaseDialect) GetServerVersionQuery() string {
	return "SELECT 'version', pg_catalog.VERSION()"
}

func (m *PgDatabaseDialect) GetIsReaderQuery() string {
	return "SELECT pg_catalog.pg_is_in_recovery()"
}

func (p *PgDatabaseDialect) IsDialect(conn driver.Conn) bool {
	return utils.CheckExistenceQueries(conn, "SELECT 1 FROM pg_catalog.pg_proc LIMIT 1")
}

func (p *PgDatabaseDialect) GetRowParser() RowParser {
	return PgRowParser
}

func (p *PgDatabaseDialect) GetHostListProvider(
	props *utils.RWMap[string, string],
	hostListProviderService HostListProviderService,
	_ PluginService) HostListProvider {
	return NewDsnHostListProvider(props, hostListProviderService)
}

func (p *PgDatabaseDialect) DoesStatementSetAutoCommit(_ string) (bool, bool) {
	return false, false
}

func (p *PgDatabaseDialect) DoesStatementSetCatalog(_ string) (string, bool) {
	return "", false
}

func (p *PgDatabaseDialect) DoesStatementSetReadOnly(statement string) (bool, bool) {
	lowercaseStatement := strings.ToLower(statement)
	if strings.HasPrefix(lowercaseStatement, "set session characteristics as transaction read only") {
		return true, true
	}

	if strings.HasPrefix(lowercaseStatement, "set session characteristics as transaction read write") {
		return false, true
	}

	return false, false
}

func (p *PgDatabaseDialect) DoesStatementSetSchema(statement string) (string, bool) {
	re := regexp.MustCompile(`(?i)set search_path( to |\s?=\s?)("?.+"?)`)
	matches := re.FindStringSubmatch(statement)
	if len(matches) < 3 {
		return "", false
	}
	schema := strings.TrimSpace(matches[2])
	if schema[0] == '"' && schema[len(schema)-1] == '"' {
		return schema[1 : len(schema)-1], true
	}
	return schema, true
}

func (p *PgDatabaseDialect) DoesStatementSetTransactionIsolation(statement string) (TransactionIsolationLevel, bool) {
	lowercaseStatement := strings.ToLower(statement)
	if strings.Contains(lowercaseStatement, "set session characteristics as transaction isolation level read uncommitted") {
		return TRANSACTION_READ_UNCOMMITTED, true
	}
	if strings.Contains(lowercaseStatement, "set session characteristics as transaction isolation level read committed") {
		return TRANSACTION_READ_COMMITTED, true
	}
	if strings.Contains(lowercaseStatement, "set session characteristics as transaction isolation level repeatable read") {
		return TRANSACTION_REPEATABLE_READ, true
	}
	if strings.Contains(lowercaseStatement, "set session characteristics as transaction isolation level serializable") {
		return TRANSACTION_SERIALIZABLE, true
	}

	return TRANSACTION_READ_UNCOMMITTED, false
}

func (p *PgDatabaseDialect) GetSetAutoCommitQuery(_ bool) (string, error) {
	return "", error_util.NewGenericAwsWrapperError(error_util.GetMessage("AwsWrapper.unsupportedMethodError", "SetAutoCommit", fmt.Sprintf("%T", p)))
}

func (p *PgDatabaseDialect) GetSetReadOnlyQuery(readOnly bool) (string, error) {
	readOnlyStr := "only"
	if !readOnly {
		readOnlyStr = "write"
	}
	return fmt.Sprintf("set session characteristics as transaction read %v", readOnlyStr), nil
}

func (p *PgDatabaseDialect) GetSetCatalogQuery(_ string) (string, error) {
	return "", error_util.NewGenericAwsWrapperError(error_util.GetMessage("AwsWrapper.unsupportedMethodError", "SetCatalog", fmt.Sprintf("%T", p)))
}

func (p *PgDatabaseDialect) GetSetSchemaQuery(schema string) (string, error) {
	if strings.Contains(schema, " ") && !strings.HasPrefix(schema, "\"") && !strings.HasSuffix(schema, "\"") {
		return fmt.Sprintf("set search_path to \"%v\"", schema), nil
	}
	return fmt.Sprintf("set search_path to %v", schema), nil
}

func (p *PgDatabaseDialect) GetSetTransactionIsolationQuery(level TransactionIsolationLevel) (string, error) {
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
	return fmt.Sprintf("set session characteristics as transaction isolation level %v", levelStr), nil
}

type RdsPgDatabaseDialect struct {
	PgDatabaseDialect
}

func (m *RdsPgDatabaseDialect) GetDialectUpdateCandidates() []string {
	return []string{RDS_PG_MULTI_AZ_CLUSTER_DIALECT, AURORA_PG_DIALECT, RDS_PG_DIALECT}
}

func (m *RdsPgDatabaseDialect) IsDialect(conn driver.Conn) bool {
	if !m.PgDatabaseDialect.IsDialect(conn) {
		return false
	}
	hasExtensions := utils.GetFirstRowFromQuery(
		conn,
		"SELECT (setting LIKE '%rds_tools%') AS rds_tools, (setting LIKE '%aurora_stat_utils%') AS aurora_stat_utils FROM pg_catalog.pg_settings "+
			"WHERE name OPERATOR(pg_catalog.=) 'rds.extensions'")
	return hasExtensions != nil && hasExtensions[0] == true && // If a variables with such name is present then it is an RDS cluster.
		hasExtensions[1] == false // If aurora_stat_utils is present then it should be treated as an Aurora cluster, not an RDS cluster.
}

func (m *RdsPgDatabaseDialect) GetBlueGreenStatus() string {
	return "SELECT version, endpoint, port, role, status FROM rds_tools.show_topology('aws_advanced_go_wrapper-" + driver_info.AWS_ADVANCED_GO_WRAPPER_VERSION + "')"
}

func (m *RdsPgDatabaseDialect) IsBlueGreenStatusAvailable(conn driver.Conn) bool {
	topologyTableExistQuery := "SELECT 'rds_tools.show_topology'::regproc"
	return utils.CheckExistenceQueries(conn, topologyTableExistQuery)
}

type AuroraPgDatabaseDialect struct {
	PgDatabaseDialect
}

func (m *AuroraPgDatabaseDialect) GetTopologyQuery() string {
	return "SELECT server_id, CASE WHEN SESSION_ID OPERATOR(pg_catalog.=) 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END AS is_writer, " +
		"CPU, COALESCE(REPLICA_LAG_IN_MSEC, 0) AS lag, LAST_UPDATE_TIMESTAMP " +
		"FROM pg_catalog.aurora_replica_status() " +
		// Filter out hosts that haven't been updated in the last 5 minutes.
		"WHERE EXTRACT(EPOCH FROM(pg_catalog.NOW() OPERATOR(pg_catalog.-) LAST_UPDATE_TIMESTAMP)) OPERATOR(pg_catalog.<=) 300 OR SESSION_ID OPERATOR(pg_catalog.=) " +
		"'MASTER_SESSION_ID' OR LAST_UPDATE_TIMESTAMP IS NULL"
}

func (m *AuroraPgDatabaseDialect) GetInstanceIdQuery() string {
	return "SELECT pg_catalog.aurora_db_instance_identifier()"
}

func (m *AuroraPgDatabaseDialect) GetWriterIdQuery() string {
	return "SELECT server_id FROM pg_catalog.aurora_replica_status() WHERE SESSION_ID OPERATOR(pg_catalog.=) 'MASTER_SESSION_ID' AND SERVER_ID OPERATOR(pg_catalog.=)" +
		" pg_catalog.aurora_db_instance_identifier()"
}

func (m *AuroraPgDatabaseDialect) GetDialectUpdateCandidates() []string {
	return []string{RDS_PG_MULTI_AZ_CLUSTER_DIALECT}
}

func (m *AuroraPgDatabaseDialect) IsDialect(conn driver.Conn) bool {
	if !m.PgDatabaseDialect.IsDialect(conn) {
		return false
	}
	hasExtensions := utils.GetFirstRowFromQuery(
		conn,
		"SELECT (setting LIKE '%aurora_stat_utils%') AS aurora_stat_utils FROM pg_catalog.pg_settings WHERE name OPERATOR(pg_catalog.=) 'rds.extensions'")
	hasTopology := utils.GetFirstRowFromQuery(conn, "SELECT 1 FROM pg_catalog.aurora_replica_status() LIMIT 1")
	// If both variables with such name are presented then it means it's an Aurora cluster.
	return hasExtensions != nil && hasExtensions[0] == true && hasTopology != nil
}

func (m *AuroraPgDatabaseDialect) GetHostListProvider(
	props *utils.RWMap[string, string],
	hostListProviderService HostListProviderService,
	pluginService PluginService) HostListProvider {
	return NewRdsHostListProvider(hostListProviderService, m, NewAuroraTopologyUtils(m), props, pluginService)
}

func (m *AuroraPgDatabaseDialect) GetLimitlessRouterEndpointQuery() string {
	return "select router_endpoint, load from pg_catalog.aurora_limitless_router_endpoints()"
}

func (m *AuroraPgDatabaseDialect) GetBlueGreenStatusQuery() string {
	return "SELECT version, endpoint, port, role, status FROM pg_catalog.get_blue_green_fast_switchover_metadata(" +
		"'aws_advanced_go_wrapper-" + driver_info.AWS_ADVANCED_GO_WRAPPER_VERSION + "')"
}

func (m *AuroraPgDatabaseDialect) IsBlueGreenStatusAvailable(conn driver.Conn) bool {
	topologyTableExistQuery := "SELECT 'pg_catalog.get_blue_green_fast_switchover_metadata'::regproc"
	return utils.CheckExistenceQueries(conn, topologyTableExistQuery)
}

// type GlobalAuroraPgDatabaseDialect struct {
// 	AuroraPgDatabaseDialect
// }

// func (g *GlobalAuroraPgDatabaseDialect) IsDialect(conn driver.Conn) bool {

// }

// func (g *GlobalAuroraPgDatabaseDialect) GetDialectUpdateCandidates() []string {
// 	return []string{}
// }

// func (g* GlobalAuroraPgDatabaseDialect)
type RdsMultiAzClusterPgDatabaseDialect struct {
	PgDatabaseDialect
}

func (r *RdsMultiAzClusterPgDatabaseDialect) IsDialect(conn driver.Conn) bool {
	writerHostFuncExistQuery := "SELECT 1 AS tmp FROM information_schema.routines WHERE routine_schema OPERATOR(pg_catalog.=) 'rds_tools' " +
		"AND routine_name OPERATOR(pg_catalog.=) 'multi_az_db_cluster_source_dbi_resource_id'"
	fetchWriterHostQuery := "SELECT multi_az_db_cluster_source_dbi_resource_id FROM rds_tools.multi_az_db_cluster_source_dbi_resource_id()"

	if utils.GetFirstRowFromQueryAsString(conn, writerHostFuncExistQuery) == nil {
		return false
	}

	row := utils.GetFirstRowFromQuery(conn, fetchWriterHostQuery)
	if len(row) == 0 {
		return false
	}
	return row[0] != nil
}

func (r *RdsMultiAzClusterPgDatabaseDialect) GetDialectUpdateCandidates() []string {
	return []string{}
}

func (r *RdsMultiAzClusterPgDatabaseDialect) GetTopologyQuery() string {
	return fmt.Sprintf("SELECT id, endpoint FROM rds_tools.show_topology('aws-advanced-go-wrapper-%v')", driver_info.AWS_ADVANCED_GO_WRAPPER_VERSION)
}

func (r *RdsMultiAzClusterPgDatabaseDialect) GetInstanceIdQuery() string {
	return "SELECT id, SUBSTRING(endpoint FROM 0 FOR POSITION('.' IN endpoint))" +
		" FROM rds_tools.show_topology()" +
		" WHERE id OPERATOR(pg_catalog.=) rds_tools.dbi_resource_id()"
}

func (r *RdsMultiAzClusterPgDatabaseDialect) GetWriterIdQuery() string {
	return "SELECT multi_az_db_cluster_source_dbi_resource_id " +
		"FROM rds_tools.multi_az_db_cluster_source_dbi_resource_id()"
}

func (r *RdsMultiAzClusterPgDatabaseDialect) GetHostListProvider(
	props *utils.RWMap[string, string],
	hostListProviderService HostListProviderService,
	pluginService PluginService) HostListProvider {
	return NewRdsHostListProvider(hostListProviderService, r, NewMultiAzTopologyUtils(r), props, pluginService)
}

func (r *RdsMultiAzClusterPgDatabaseDialect) GetWriterIdColumnName() string {
	return "multi_az_db_cluster_source_dbi_resource_id"
}
