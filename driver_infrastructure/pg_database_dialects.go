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
	"awssql/utils"
	"context"
	"database/sql/driver"
	"fmt"
	"time"
)

type PgDatabaseDialect struct {
}

func (p *PgDatabaseDialect) GetDialectUpdateCandidates() []string {
	return []string{AURORA_PG_DIALECT, RDS_PG_DIALECT}
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

func (p *PgDatabaseDialect) GetSetAutoCommitQuery(autoCommit bool) (string, error) {
	return "", error_util.NewUnsupportedMethodError("setAutoCommit", fmt.Sprintf("%T", p))
}

func (p *PgDatabaseDialect) GetSetCatalogQuery(catalog string) (string, error) {
	return "", error_util.NewUnsupportedMethodError("setCatalog", fmt.Sprintf("%T", p))
}

func (p *PgDatabaseDialect) GetSetReadOnlyQuery(readOnly bool) string {
	if readOnly {
		return "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY"
	}
	return "SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE"
}

func (p *PgDatabaseDialect) GetSetSchemaQuery(schema string) (string, error) {
	return fmt.Sprintf("SET search_path TO %s", schema), nil
}

func (p *PgDatabaseDialect) GetSetTransactionIsolationQuery(level TransactionIsolationLevel) (string, error) {
	var transactionIsolationLevel string
	switch level {
	case TRANSACTION_READ_UNCOMMITTED:
		transactionIsolationLevel = "READ UNCOMMITTED"
	case TRANSACTION_READ_COMMITTED:
		transactionIsolationLevel = "READ COMMITTED"
	case TRANSACTION_REPEATABLE_READ:
		transactionIsolationLevel = "REPEATABLE READ"
	case TRANSACTION_SERIALIZABLE:
		transactionIsolationLevel = "SERIALIZABLE"
	default:
		return "", error_util.NewGenericAwsWrapperError(error_util.GetMessage("Conn.invalidTransactionIsolationLevel", level))
	}
	return fmt.Sprintf("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL %s", transactionIsolationLevel), nil
}

func (p *PgDatabaseDialect) IsDialect(conn driver.Conn) bool {
	row := utils.GetFirstRowFromQuery(conn, "SELECT 1 FROM pg_proc LIMIT 1")
	// If the pg_proc table exists then it's a PostgreSQL cluster.
	return row != nil
}

func (m *PgDatabaseDialect) GetHostListProvider(
	props map[string]string,
	initialDsn string,
	hostListProviderService HostListProviderService) HostListProvider {
	return HostListProvider(NewDsnHostListProvider(props, initialDsn, hostListProviderService))
}

type RdsPgDatabaseDialect struct {
	PgDatabaseDialect
}

func (m *RdsPgDatabaseDialect) GetDialectUpdateCandidates() []string {
	return []string{AURORA_PG_DIALECT}
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

type AuroraPgDatabaseDialect struct {
	PgDatabaseDialect
}

func (m *AuroraPgDatabaseDialect) GetDialectUpdateCandidates() []string {
	return []string{}
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

func (m *AuroraPgDatabaseDialect) GetHostListProvider(props map[string]string, initialDsn string, hostListProviderService HostListProviderService) HostListProvider {
	return HostListProvider(NewRdsHostListProvider(hostListProviderService, m, props, initialDsn))
}

func (m *AuroraPgDatabaseDialect) GetHostRole(conn driver.Conn) host_info_util.HostRole {
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

func (m *AuroraPgDatabaseDialect) GetTopology(conn driver.Conn, provider *RdsHostListProvider) []host_info_util.HostInfo {
	topologyQuery := "SELECT server_id, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END AS is_writer, " +
		"CPU, COALESCE(REPLICA_LAG_IN_MSEC, 0) AS lag, LAST_UPDATE_TIMESTAMP " +
		"FROM aurora_replica_status() " +
		// Filter out hosts that haven't been updated in the last 5 minutes.
		"WHERE EXTRACT(EPOCH FROM(NOW() - LAST_UPDATE_TIMESTAMP)) <= 300 OR SESSION_ID = 'MASTER_SESSION_ID' " +
		"OR LAST_UPDATE_TIMESTAMP IS NULL"

	queryerCtx, ok := conn.(driver.QueryerContext)
	if !ok {
		// Unable to query, conn does not implement QueryerContext.
		return nil
	}

	rows, err := queryerCtx.QueryContext(context.Background(), topologyQuery, nil)
	if err != nil {
		// Query failed.
		return nil
	}

	hosts := []host_info_util.HostInfo{}
	if rows == nil {
		return hosts
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
		hosts = append(hosts, provider.createHost(hostName, hostRole, lag, cpu, lastUpdateTime))
		err = rows.Next(row)
	}
	rows.Close()
	return hosts
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
