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
	"awssql/utils"
	"database/sql/driver"
	"fmt"
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
	return "", error_util.NewUnsupportedMethodError(error_util.GetMessage("Conn.unsupportedMethodError", "setAutoCommit"))
}

func (p *PgDatabaseDialect) GetSetCatalogQuery(catalog string) (string, error) {
	return "", error_util.NewUnsupportedMethodError(error_util.GetMessage("Conn.unsupportedMethodError", "setCatalog"))
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
	hostListProviderService HostListProviderService) *HostListProvider {
	provider := HostListProvider(NewDsnHostListProvider(props, initialDsn, hostListProviderService))
	return &provider
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

func (m *AuroraPgDatabaseDialect) GetHostListProvider(
	props map[string]string,
	initialDsn string,
	hostListProviderService HostListProviderService) *HostListProvider {
	// TODO: implement GetHostListProvider, see ticket: "dev: RdsHostListProvider".
	panic("implement me")
}
