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
	"awssql/error_util"
	"awssql/host_info_util"
	"awssql/property_util"
	"awssql/utils"
	"context"
	"database/sql/driver"
	"log/slog"
	"strings"
	"time"
)

type MySQLDatabaseDialect struct {
}

func (m *MySQLDatabaseDialect) GetDialectUpdateCandidates() []string {
	return []string{AURORA_MYSQL_DIALECT, RDS_MYSQL_DIALECT}
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

type RdsMySQLDatabaseDialect struct {
	MySQLDatabaseDialect
}

func (m *RdsMySQLDatabaseDialect) GetDialectUpdateCandidates() []string {
	return []string{AURORA_MYSQL_DIALECT}
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

type AuroraMySQLDatabaseDialect struct {
	MySQLDatabaseDialect
}

func (m *AuroraMySQLDatabaseDialect) GetDialectUpdateCandidates() []string {
	return []string{}
}

func (m *AuroraMySQLDatabaseDialect) IsDialect(conn driver.Conn) bool {
	row := utils.GetFirstRowFromQueryAsString(conn, "SHOW VARIABLES LIKE 'aurora_version'")
	// If a variable with such name is presented then it means it's an Aurora cluster.
	return row != nil
}

func (m *AuroraMySQLDatabaseDialect) GetHostListProvider(
	props map[string]string,
	initialDsn string,
	hostListProviderService HostListProviderService,
	pluginService PluginService) HostListProvider {
	pluginsProp := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.PLUGINS)

	if strings.Contains(pluginsProp, "failover") {
		slog.Debug("Failover is enabled. Using MonitoringRdsHostListProvider")
		return HostListProvider(NewMonitoringRdsHostListProvider(hostListProviderService, m, props, initialDsn, pluginService))
	}

	slog.Debug("Failover NOT enabled. Using RdsHostListProvider")
	return HostListProvider(NewRdsHostListProvider(hostListProviderService, m, props, initialDsn, nil, nil))
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

func (m *AuroraMySQLDatabaseDialect) GetHostRole(conn driver.Conn) host_info_util.HostRole {
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
