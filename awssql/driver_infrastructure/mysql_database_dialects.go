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
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
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
	props map[string]string,
	initialDsn string,
	hostListProviderService HostListProviderService,
	pluginService PluginService) HostListProvider {
	return HostListProvider(NewDsnHostListProvider(props, initialDsn, hostListProviderService))
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

func (m *MySQLDatabaseDialect) GetSetSchemaQuery(schema string) (string, error) {
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

func (m *MySQLDatabaseDialect) DoesStatementSetSchema(statement string) (string, bool) {
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

type MySQLTopologyAwareDatabaseDialect struct {
	MySQLDatabaseDialect
}

func (m *MySQLTopologyAwareDatabaseDialect) GetTopology(
	conn driver.Conn, provider HostListProvider) ([]*host_info_util.HostInfo, error) {
	return nil, nil
}

func (m *MySQLTopologyAwareDatabaseDialect) GetHostRole(conn driver.Conn) host_info_util.HostRole {
	isReaderQuery := "SELECT @@innodb_read_only"
	res := utils.GetFirstRowFromQuery(conn, isReaderQuery)
	if len(res) > 0 {
		isReader, ok := res[0].(int64)
		if ok {
			if isReader == 1 {
				return host_info_util.READER
			}
			return host_info_util.WRITER
		}
	}
	return host_info_util.UNKNOWN
}

func (m *MySQLTopologyAwareDatabaseDialect) GetHostName(conn driver.Conn) string {
	return ""
}

func (m *MySQLTopologyAwareDatabaseDialect) GetWriterHostName(conn driver.Conn) (string, error) {
	return "", nil
}

func (m *MySQLTopologyAwareDatabaseDialect) GetHostListProvider(
	props map[string]string,
	initialDsn string,
	hostListProviderService HostListProviderService,
	pluginService PluginService) HostListProvider {
	return m.getTopologyAwareHostListProvider(m, props, initialDsn, hostListProviderService, pluginService)
}

func (m *MySQLTopologyAwareDatabaseDialect) getTopologyAwareHostListProvider(
	dialect TopologyAwareDialect,
	props map[string]string,
	initialDsn string,
	hostListProviderService HostListProviderService,
	pluginService PluginService) HostListProvider {
	pluginsProp := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.PLUGINS)

	if strings.Contains(pluginsProp, "failover") {
		slog.Debug(error_util.GetMessage("DatabaseDialect.usingMonitoringHostListProvider"))
		return HostListProvider(NewMonitoringRdsHostListProvider(hostListProviderService, dialect, props, initialDsn, pluginService))
	}

	slog.Debug(error_util.GetMessage("DatabaseDialect.usingRdsHostListProvider"))
	return HostListProvider(NewRdsHostListProvider(hostListProviderService, dialect, props, initialDsn, nil, nil))
}

type AuroraMySQLDatabaseDialect struct {
	MySQLTopologyAwareDatabaseDialect
}

func (m *AuroraMySQLDatabaseDialect) GetDialectUpdateCandidates() []string {
	return []string{RDS_MYSQL_MULTI_AZ_CLUSTER_DIALECT}
}

func (m *AuroraMySQLDatabaseDialect) IsDialect(conn driver.Conn) bool {
	row := utils.GetFirstRowFromQueryAsString(conn, "SHOW VARIABLES LIKE 'aurora_version'")
	// If a variable with such name is presented then it means it's an Aurora cluster.
	return row != nil
}

func (m *AuroraMySQLDatabaseDialect) GetHostName(conn driver.Conn) string {
	hostIdQuery := "SELECT @@aurora_server_id"
	res := utils.GetFirstRowFromQueryAsString(conn, hostIdQuery)
	if len(res) > 0 {
		return res[0]
	}
	return ""
}

func (m *AuroraMySQLDatabaseDialect) GetWriterHostName(conn driver.Conn) (string, error) {
	hostIdQuery := "SELECT server_id " +
		"FROM information_schema.replica_host_status " +
		"WHERE SESSION_ID = 'MASTER_SESSION_ID' AND SERVER_ID = @@aurora_server_id"
	res := utils.GetFirstRowFromQueryAsString(conn, hostIdQuery)
	if res == nil {
		return "", error_util.NewGenericAwsWrapperError("Could not determine writer host name.")
	}
	if len(res) > 0 {
		return res[0], nil
	}
	return "", nil
}

func (m *AuroraMySQLDatabaseDialect) GetHostListProvider(
	props map[string]string,
	initialDsn string,
	hostListProviderService HostListProviderService,
	pluginService PluginService) HostListProvider {
	return m.getTopologyAwareHostListProvider(m, props, initialDsn, hostListProviderService, pluginService)
}

func (m *AuroraMySQLDatabaseDialect) GetTopology(conn driver.Conn, provider HostListProvider) ([]*host_info_util.HostInfo, error) {
	topologyQuery := "SELECT server_id, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END as is_writer, " +
		"cpu, REPLICA_LAG_IN_MILLISECONDS as 'lag', LAST_UPDATE_TIMESTAMP as last_update_timestamp " +
		"FROM information_schema.replica_host_status " +
		// Filter out hosts that haven't been updated in the last 5 minutes.
		"WHERE time_to_sec(timediff(now(), LAST_UPDATE_TIMESTAMP)) <= 300 OR SESSION_ID = 'MASTER_SESSION_ID' "

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

	hosts := []*host_info_util.HostInfo{}
	if rows == nil {
		// Query returned an empty host list, no processing required.
		return hosts, nil
	}
	row := make([]driver.Value, len(rows.Columns()))
	err = rows.Next(row)

	for err == nil && len(row) > 4 {
		hostNameAsInt, ok1 := row[0].([]uint8)
		isWriterAsInt, ok2 := row[1].(int64)
		cpu, ok3 := row[2].(float64)
		lag, ok4 := row[3].(float64)
		lastUpdateTimeAsInt, ok5 := row[4].([]uint8)
		if !ok1 || !ok2 || !ok3 || !ok4 {
			// Unable to use information from row to create a host, try next row.
			err = rows.Next(row)
			continue
		}

		var lastUpdateTime time.Time
		if ok5 {
			lastUpdateTime, err = time.Parse("2006-01-02 15:04:05.999999", string(lastUpdateTimeAsInt))
		}
		if !ok5 || err != nil {
			// Unable to get or convert last update time, use current time.
			lastUpdateTime = time.Now()
		}
		role := host_info_util.READER
		if isWriterAsInt == 1 {
			role = host_info_util.WRITER
		}
		hosts = append(hosts, provider.CreateHost(string(hostNameAsInt), role, lag, cpu, lastUpdateTime))
		err = rows.Next(row)
	}
	return hosts, nil
}

type RdsMultiAzClusterMySQLDatabaseDialect struct {
	MySQLTopologyAwareDatabaseDialect
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

func (r *RdsMultiAzClusterMySQLDatabaseDialect) GetTopology(conn driver.Conn, provider HostListProvider) ([]*host_info_util.HostInfo, error) {
	topologyQuery := "SELECT id, endpoint FROM mysql.rds_topology"
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

func (r *RdsMultiAzClusterMySQLDatabaseDialect) processTopologyQueryResults(
	provider HostListProvider,
	writerHostId string,
	rows driver.Rows) []*host_info_util.HostInfo {
	hosts := []*host_info_util.HostInfo{}
	row := make([]driver.Value, len(rows.Columns()))
	err := rows.Next(row)
	for err == nil && len(row) > 1 {
		id, ok1 := row[0].(int64)
		endpoint, ok2 := row[1].([]uint8)
		if !ok1 || !ok2 {
			// Unable to use information from row to create a host.
			err = rows.Next(row)
			continue
		}
		idString := strconv.FormatInt(id, 10)
		endpointString := string(endpoint)
		hostRole := host_info_util.READER

		if writerHostId == idString {
			hostRole = host_info_util.WRITER
		}

		hostName := utils.GetHostNameFromEndpoint(endpointString)
		hosts = append(hosts, provider.CreateHost(hostName, hostRole, 0, 0, time.Now()))
		err = rows.Next(row)
	}
	return hosts
}

func (r *RdsMultiAzClusterMySQLDatabaseDialect) GetHostName(conn driver.Conn) string {
	hostNameQuery := "SELECT endpoint from mysql.rds_topology as top where top.id = (SELECT @@server_id)"
	row := utils.GetFirstRowFromQueryAsString(conn, hostNameQuery)

	if len(row) > 0 {
		return utils.GetHostNameFromEndpoint(row[0])
	}

	return ""
}

func (r *RdsMultiAzClusterMySQLDatabaseDialect) getWriterHostId(conn driver.Conn) string {
	fetchWriterHostQuery := "SHOW REPLICA STATUS"
	queryerCtx, ok := conn.(driver.QueryerContext)
	if !ok {
		// Unable to query, conn does not implement QueryerContext.
		return ""
	}

	rows, err := queryerCtx.QueryContext(context.Background(), fetchWriterHostQuery, nil)
	if err != nil {
		return ""
	}
	defer rows.Close()

	// Get column names
	columnNames := rows.Columns()

	// Read first row. If there is nothing,
	//  then we should return the current server id
	values := make([]driver.Value, len(columnNames))
	if err := rows.Next(values); err != nil {
		return r.getHostIdOfCurrentConnection(conn)
	}

	var sourceIndex int = -1
	for i, name := range columnNames {
		if name == "Source_Server_Id" {
			sourceIndex = i
			break
		}
	}
	if sourceIndex == -1 {
		return ""
	}

	writerHostId := values[sourceIndex]
	writerHostIdInt, ok := writerHostId.(int64)
	if !ok {
		return ""
	}
	return strconv.FormatInt(writerHostIdInt, 10)
}

func (r *RdsMultiAzClusterMySQLDatabaseDialect) GetWriterHostName(conn driver.Conn) (string, error) {
	writerId := r.getWriterHostId(conn)
	if writerId == "" {
		return "", error_util.NewGenericAwsWrapperError("Could not determine writer host name.")
	}

	fetchEndpointQuery := fmt.Sprintf("SELECT endpoint from mysql.rds_topology as top where top.id = '%v' and top.id = (SELECT @@server_id)", writerId)
	res := utils.GetFirstRowFromQuery(conn, fetchEndpointQuery)
	if res == nil {
		return "", error_util.NewGenericAwsWrapperError("Could not determine writer host name.")
	}

	if len(res) > 0 {
		instanceId, ok := (res[0]).([]uint8)

		if ok {
			return utils.GetHostNameFromEndpoint(string(instanceId)), nil
		}
	}
	return "", nil
}

func (r *RdsMultiAzClusterMySQLDatabaseDialect) getHostIdOfCurrentConnection(conn driver.Conn) string {
	serverIdQuery := "SELECT @@server_id"
	row := utils.GetFirstRowFromQueryAsString(conn, serverIdQuery)

	if len(row) > 0 {
		return row[0]
	}

	return ""
}

func (r *RdsMultiAzClusterMySQLDatabaseDialect) GetHostListProvider(
	props map[string]string,
	initialDsn string,
	hostListProviderService HostListProviderService,
	pluginService PluginService) HostListProvider {
	return r.getTopologyAwareHostListProvider(r, props, initialDsn, hostListProviderService, pluginService)
}
