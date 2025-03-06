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

package driver

import (
	"database/sql/driver"
	"fmt"
	"strings"
)

type MySQLDatabaseDialect struct {
}

func (m *MySQLDatabaseDialect) GetDialectUpdateCandidates() []string {
	return []string{AURORA_MYSQL_DIALECT, RDS_MYSQL_DIALECT}
}

func (m *MySQLDatabaseDialect) GetSetAutoCommitQuery(autoCommit bool) (string, error) {
	return fmt.Sprintf("SET AUTOCOMMIT=%t", autoCommit), nil
}

func (m *MySQLDatabaseDialect) GetDefaultPort() int {
	return 3306
}

func (m *MySQLDatabaseDialect) GetHostAliasQuery() string {
	return "SELECT CONCAT(@@hostname, ':', @@port)"
}

func (m *MySQLDatabaseDialect) GetSetReadOnlyQuery(readOnly bool) string {
	if readOnly {
		return "SET SESSION TRANSACTION READ ONLY"
	}
	return "SET SESSION TRANSACTION READ WRITE"
}

func (m *MySQLDatabaseDialect) GetServerVersionQuery() string {
	return "SHOW VARIABLES LIKE 'version_comment'"
}

func (m *MySQLDatabaseDialect) GetSetCatalogQuery(catalog string) (string, error) {
	return fmt.Sprintf("USE %s", catalog), nil
}

func (m *MySQLDatabaseDialect) GetSetSchemaQuery(schema string) (string, error) {
	return "", NewUnsupportedMethodError("setSchema")
}

func (m *MySQLDatabaseDialect) GetSetTransactionIsolationQuery(level TransactionIsolationLevel) (string, error) {
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
		return "", NewGenericAwsWrapperError(GetMessage("Conn.invalidTransactionIsolationLevel", level))
	}
	return fmt.Sprintf("SET SESSION TRANSACTION ISOLATION LEVEL %s", transactionIsolationLevel), nil
}

func (m *MySQLDatabaseDialect) IsDialect(conn driver.Conn) bool {
	// MysqlDialect and RdsMysqlDialect use the same server version query to determine the dialect.
	// The `SHOW VARIABLES LIKE 'version_comment'` outputs
	// | Variable_name   | value                        |
	// |-----------------|------------------------------|
	// | version_comment | MySQL Community Server (GPL) |
	// for community Mysql.
	row := GetFirstRowFromQueryAsString(conn, m.GetServerVersionQuery())
	if len(row) > 1 && strings.Contains(row[1], "MySQL") {
		return true
	}
	return false
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
	row := GetFirstRowFromQueryAsString(conn, m.GetServerVersionQuery())
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
	row := GetFirstRowFromQueryAsString(conn, "SHOW VARIABLES LIKE 'aurora_version'")
	// If a variable with such name is presented then it means it's an Aurora cluster.
	return row != nil
}
