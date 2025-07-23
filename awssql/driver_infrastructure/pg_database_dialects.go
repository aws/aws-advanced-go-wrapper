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
	"strings"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_info"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
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
	return "SELECT CONCAT(inet_server_addr(), ':', inet_server_port())"
}

func (p *PgDatabaseDialect) GetServerVersionQuery() string {
	return "SELECT 'version', VERSION()"
}

func (p *PgDatabaseDialect) IsDialect(conn driver.Conn) bool {
	row := utils.GetFirstRowFromQuery(conn, "SELECT 1 FROM pg_proc LIMIT 1")
	// If the pg_proc table exists then it's a PostgreSQL cluster.
	return row != nil
}

func (p *PgDatabaseDialect) GetHostListProvider(
	props map[string]string,
	hostListProviderService HostListProviderService,
	pluginService PluginService) HostListProvider {
	return NewDsnHostListProvider(props, hostListProviderService)
}

func (p *PgDatabaseDialect) DoesStatementSetAutoCommit(statement string) (bool, bool) {
	return false, false
}

func (p *PgDatabaseDialect) DoesStatementSetCatalog(statement string) (string, bool) {
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

func (p *PgDatabaseDialect) GetSetAutoCommitQuery(autoCommit bool) (string, error) {
	return "", error_util.NewGenericAwsWrapperError(error_util.GetMessage("AwsWrapper.unsupportedMethodError", "SetAutoCommit", fmt.Sprintf("%T", p)))
}

func (p *PgDatabaseDialect) GetSetReadOnlyQuery(readOnly bool) (string, error) {
	readOnlyStr := "only"
	if !readOnly {
		readOnlyStr = "write"
	}
	return fmt.Sprintf("set session characteristics as transaction read %v", readOnlyStr), nil
}

func (p *PgDatabaseDialect) GetSetCatalogQuery(catalog string) (string, error) {
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
		"SELECT (setting LIKE '%rds_tools%') AS rds_tools, (setting LIKE '%aurora_stat_utils%') AS aurora_stat_utils FROM pg_settings "+
			"WHERE name='rds.extensions'")
	return hasExtensions != nil && hasExtensions[0] == true && // If a variables with such name is present then it is an RDS cluster.
		hasExtensions[1] == false // If aurora_stat_utils is present then it should be treated as an Aurora cluster, not an RDS cluster.
}

func (m *RdsPgDatabaseDialect) GetBlueGreenStatus(conn driver.Conn) []BlueGreenResult {
	bgStatusQuery := "SELECT * FROM rds_tools.show_topology('aws_advanced_go_wrapper-" + driver_info.AWS_ADVANCED_GO_WRAPPER_VERSION + "')"
	return pgGetBlueGreenStatus(conn, bgStatusQuery)
}

func (m *RdsPgDatabaseDialect) IsBlueGreenStatusAvailable(conn driver.Conn) bool {
	topologyTableExistQuery := "SELECT 'rds_tools.show_topology'::regproc"
	return utils.GetFirstRowFromQuery(conn, topologyTableExistQuery) != nil
}

type PgTopologyAwareDatabaseDialect struct {
	PgDatabaseDialect
}

func (m *PgTopologyAwareDatabaseDialect) GetTopology(
	conn driver.Conn, provider HostListProvider) ([]*host_info_util.HostInfo, error) {
	return nil, nil
}

func (m *PgTopologyAwareDatabaseDialect) GetHostRole(conn driver.Conn) host_info_util.HostRole {
	isReaderQuery := "SELECT pg_is_in_recovery()"
	res := utils.GetFirstRowFromQuery(conn, isReaderQuery)
	if len(res) > 0 {
		b, ok := (res[0]).(bool)
		if ok {
			if b {
				return host_info_util.READER
			}
			return host_info_util.WRITER
		}
	}
	return host_info_util.UNKNOWN
}

func (m *PgTopologyAwareDatabaseDialect) GetHostName(conn driver.Conn) string {
	return ""
}
func (m *PgTopologyAwareDatabaseDialect) GetWriterHostName(conn driver.Conn) (string, error) {
	return "", nil
}

func (m *PgTopologyAwareDatabaseDialect) GetHostListProvider(
	props map[string]string,
	hostListProviderService HostListProviderService,
	pluginService PluginService) HostListProvider {
	return m.getTopologyAwareHostListProvider(m, props, hostListProviderService, pluginService)
}

func (m *PgTopologyAwareDatabaseDialect) getTopologyAwareHostListProvider(
	dialect TopologyAwareDialect,
	props map[string]string,
	hostListProviderService HostListProviderService,
	pluginService PluginService) HostListProvider {
	pluginsProp := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.PLUGINS)

	if strings.Contains(pluginsProp, "failover") {
		slog.Debug(error_util.GetMessage("DatabaseDialect.usingMonitoringHostListProvider"))
		return NewMonitoringRdsHostListProvider(hostListProviderService, dialect, props, pluginService)
	}

	slog.Debug(error_util.GetMessage("DatabaseDialect.usingRdsHostListProvider"))
	return NewRdsHostListProvider(hostListProviderService, dialect, props, nil, nil)
}

type AuroraPgDatabaseDialect struct {
	PgTopologyAwareDatabaseDialect
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
		"SELECT (setting LIKE '%aurora_stat_utils%') AS aurora_stat_utils FROM pg_settings WHERE name='rds.extensions'")
	hasTopology := utils.GetFirstRowFromQuery(conn, "SELECT 1 FROM aurora_replica_status() LIMIT 1")
	// If both variables with such name are presented then it means it's an Aurora cluster.
	return hasExtensions != nil && hasExtensions[0] == true && hasTopology != nil
}

func (m *AuroraPgDatabaseDialect) GetTopology(conn driver.Conn, provider HostListProvider) ([]*host_info_util.HostInfo, error) {
	topologyQuery := "SELECT server_id, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END AS is_writer, " +
		"CPU, COALESCE(REPLICA_LAG_IN_MSEC, 0) AS lag, LAST_UPDATE_TIMESTAMP " +
		"FROM aurora_replica_status() " +
		// Filter out hosts that haven't been updated in the last 5 minutes.
		"WHERE EXTRACT(EPOCH FROM(NOW() - LAST_UPDATE_TIMESTAMP)) <= 300 OR SESSION_ID = 'MASTER_SESSION_ID' " +
		"OR LAST_UPDATE_TIMESTAMP IS NULL"

	queryerCtx, ok := conn.(driver.QueryerContext)
	if !ok {
		// Unable to query, conn does not implement QueryerContext.
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("Conn.doesNotImplementRequiredInterface", "driver.QueryerContext"))
	}

	rows, err := queryerCtx.QueryContext(context.Background(), topologyQuery, nil)
	if err != nil {
		// Query failed.
		return nil, err
	}
	if rows != nil {
		defer rows.Close()
	}

	var hosts []*host_info_util.HostInfo
	if rows == nil {
		// Query returned an empty host list, no processing required.
		return hosts, nil
	}
	row := make([]driver.Value, len(rows.Columns()))
	err = rows.Next(row)

	for err == nil && len(row) > 4 {
		hostName, ok1 := row[0].(string)
		isWriter, ok2 := row[1].(bool)
		cpu, ok3 := row[2].(float64)
		lag, ok4 := row[3].(float64)
		lastUpdateTime, ok5 := row[4].(time.Time)
		if !ok1 || !ok2 || !ok3 || !ok4 {
			// Unable to use information from row to create a host.
			err = rows.Next(row)
			continue
		}
		if !ok5 {
			// Not able to get last update time, use current time.
			lastUpdateTime = time.Now()
		}
		hostRole := host_info_util.READER
		if isWriter {
			hostRole = host_info_util.WRITER
		}
		hosts = append(hosts, provider.CreateHost(hostName, hostRole, lag, cpu, lastUpdateTime))
		err = rows.Next(row)
	}
	return hosts, nil
}

func (m *AuroraPgDatabaseDialect) GetHostName(conn driver.Conn) string {
	hostIdQuery := "SELECT aurora_db_instance_identifier()"
	res := utils.GetFirstRowFromQuery(conn, hostIdQuery)
	if len(res) > 0 {
		instanceId, ok := (res[0]).(string)
		if ok {
			return instanceId
		}
	}
	return ""
}

func (m *AuroraPgDatabaseDialect) GetWriterHostName(conn driver.Conn) (string, error) {
	hostIdQuery := "SELECT server_id FROM aurora_replica_status() WHERE SESSION_ID = 'MASTER_SESSION_ID' AND SERVER_ID = aurora_db_instance_identifier()"
	res := utils.GetFirstRowFromQuery(conn, hostIdQuery)
	if res == nil {
		return "", error_util.NewGenericAwsWrapperError("Could not determine writer host name.")
	}

	if len(res) > 0 {
		instanceId, ok := (res[0]).(string)
		if ok {
			return instanceId, nil
		}
	}
	return "", nil
}

func (m *AuroraPgDatabaseDialect) GetHostListProvider(
	props map[string]string,
	hostListProviderService HostListProviderService,
	pluginService PluginService) HostListProvider {
	return m.getTopologyAwareHostListProvider(m, props, hostListProviderService, pluginService)
}

func (m *AuroraPgDatabaseDialect) GetLimitlessRouterEndpointQuery() string {
	return "select router_endpoint, load from aurora_limitless_router_endpoints()"
}

func (m *AuroraPgDatabaseDialect) GetBlueGreenStatus(conn driver.Conn) []BlueGreenResult {
	bgStatusQuery := "SELECT * FROM get_blue_green_fast_switchover_metadata('aws_advanced_go_wrapper-" + driver_info.AWS_ADVANCED_GO_WRAPPER_VERSION + "')"
	return pgGetBlueGreenStatus(conn, bgStatusQuery)
}

func (m *AuroraPgDatabaseDialect) IsBlueGreenStatusAvailable(conn driver.Conn) bool {
	topologyTableExistQuery := "SELECT 'get_blue_green_fast_switchover_metadata'::regproc"
	return utils.GetFirstRowFromQuery(conn, topologyTableExistQuery) != nil
}

type RdsMultiAzClusterPgDatabaseDialect struct {
	PgTopologyAwareDatabaseDialect
}

func (r *RdsMultiAzClusterPgDatabaseDialect) IsDialect(conn driver.Conn) bool {
	writerHostFuncExistQuery := "SELECT 1 AS tmp FROM information_schema.routines WHERE routine_schema='rds_tools' " +
		"AND routine_name='multi_az_db_cluster_source_dbi_resource_id'"
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

func (r *RdsMultiAzClusterPgDatabaseDialect) GetTopology(conn driver.Conn, provider HostListProvider) ([]*host_info_util.HostInfo, error) {
	topologyQuery := fmt.Sprintf("SELECT id, endpoint FROM rds_tools.show_topology('aws-advanced-go-wrapper-%v')", driver_info.AWS_ADVANCED_GO_WRAPPER_VERSION)
	writerHostId := r.getWriterHostId(conn)

	if writerHostId == "" {
		writerHostId = r.getHostIdOfCurrentConnection(conn)
	}

	queryerCtx, ok := conn.(driver.QueryerContext)
	if !ok {
		// Unable to query, conn does not implement QueryerContext.
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("Conn.doesNotImplementRequiredInterface", "driver.QueryerContext"))
	}

	rows, err := queryerCtx.QueryContext(context.Background(), topologyQuery, nil)
	if err != nil {
		// Query failed.
		return nil, err
	}
	defer rows.Close()

	return r.processTopologyQueryResults(provider, writerHostId, rows), nil
}

func (r *RdsMultiAzClusterPgDatabaseDialect) processTopologyQueryResults(
	provider HostListProvider,
	writerHostId string,
	rows driver.Rows) []*host_info_util.HostInfo {
	var hosts []*host_info_util.HostInfo
	row := make([]driver.Value, len(rows.Columns()))
	err := rows.Next(row)
	for err == nil && len(row) > 1 {
		id, ok1 := row[0].(string)
		endpoint, ok2 := row[1].(string)
		if !ok1 || !ok2 {
			// Unable to use information from row to create a host.
			err = rows.Next(row)
			continue
		}
		hostRole := host_info_util.READER

		if writerHostId == id {
			hostRole = host_info_util.WRITER
		}

		hostName := utils.GetHostNameFromEndpoint(endpoint)
		if hostName == "" {
			// Unable to use information from row to create a host
			continue
		}
		hosts = append(hosts, provider.CreateHost(hostName, hostRole, 0, 0, time.Now()))
		err = rows.Next(row)
	}

	return hosts
}

func (r *RdsMultiAzClusterPgDatabaseDialect) getHostIdOfCurrentConnection(conn driver.Conn) string {
	hostIdQuery := "SELECT dbi_resource_id FROM rds_tools.dbi_resource_id()"

	row := utils.GetFirstRowFromQueryAsString(conn, hostIdQuery)

	if len(row) > 0 {
		return row[0]
	}

	return ""
}

func (r *RdsMultiAzClusterPgDatabaseDialect) GetHostName(conn driver.Conn) string {
	hostNameQuery := "SELECT serverid FROM rds_tools.db_instance_identifier()"
	row := utils.GetFirstRowFromQueryAsString(conn, hostNameQuery)

	if len(row) > 0 {
		return row[0]
	}

	return ""
}

func (r *RdsMultiAzClusterPgDatabaseDialect) getWriterHostId(conn driver.Conn) string {
	fetchWriterHostQuery := "SELECT multi_az_db_cluster_source_dbi_resource_id " +
		"FROM rds_tools.multi_az_db_cluster_source_dbi_resource_id()"

	row := utils.GetFirstRowFromQueryAsString(conn, fetchWriterHostQuery)
	if len(row) > 0 {
		return row[0]
	}

	return ""
}

func (r *RdsMultiAzClusterPgDatabaseDialect) GetWriterHostName(conn driver.Conn) (string, error) {
	fetchWriterHostNameQuery := "SELECT endpoint FROM rds_tools.show_topology('aws-advanced-go-wrapper') as topology " +
		"WHERE topology.id = (SELECT multi_az_db_cluster_source_dbi_resource_id FROM rds_tools.multi_az_db_cluster_source_dbi_resource_id()) " +
		"AND topology.id = (SELECT dbi_resource_id FROM rds_tools.dbi_resource_id())"

	res := utils.GetFirstRowFromQuery(conn, fetchWriterHostNameQuery)
	if res == nil {
		return "", error_util.NewGenericAwsWrapperError("Could not determine writer host name.")
	}

	if len(res) > 0 {
		endpoint, ok := (res[0]).(string)
		if !ok {
			return "", nil
		}

		hostName := utils.GetHostNameFromEndpoint(endpoint)
		if hostName != "" {
			return hostName, nil
		}
	}
	return "", nil
}

func (r *RdsMultiAzClusterPgDatabaseDialect) GetHostListProvider(
	props map[string]string,
	hostListProviderService HostListProviderService,
	pluginService PluginService) HostListProvider {
	return r.getTopologyAwareHostListProvider(r, props, hostListProviderService, pluginService)
}

func pgGetBlueGreenStatus(conn driver.Conn, query string) []BlueGreenResult {
	queryerCtx, ok := conn.(driver.QueryerContext)
	if !ok {
		// Unable to query, conn does not implement QueryerContext.
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
		if len(row) > 5 {
			version, ok1 := row[5].(string)
			endpoint, ok2 := row[1].(string)
			portAsFloat, ok3 := row[2].(int64)
			role, ok4 := row[3].(string)
			status, ok5 := row[4].(string)

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
