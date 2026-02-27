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
	"time"
)

// RowParser handles database-specific type conversions from driver.Value to Go types.
// This abstracts the differences between how PG and MySQL drivers return data.
type RowParser interface {
	// ParseString extracts a string from a driver.Value
	ParseString(val driver.Value) (string, bool)
	// ParseBool extracts a boolean from a driver.Value
	ParseBool(val driver.Value) (bool, bool)
	// ParseFloat64 extracts a float64 from a driver.Value
	ParseFloat64(val driver.Value) (float64, bool)
	// ParseTime extracts a time.Time from a driver.Value
	ParseTime(val driver.Value) (time.Time, bool)
	// ParseInt64 extracts an int64 from a driver.Value
	ParseInt64(val driver.Value) (int64, bool)
}

type DatabaseDialect interface {
	GetDefaultPort() int
	GetHostAliasQuery() string
	GetServerVersionQuery() string
	GetDialectUpdateCandidates() []string
	GetIsReaderQuery() string
	IsDialect(conn driver.Conn) bool
	GetRowParser() RowParser
	GetHostListProviderSupplier() HostListProviderSupplier
	DoesStatementSetAutoCommit(statement string) (bool, bool)
	DoesStatementSetReadOnly(statement string) (bool, bool)
	DoesStatementSetCatalog(statement string) (string, bool)
	DoesStatementSetSchema(statement string) (string, bool)
	DoesStatementSetTransactionIsolation(statement string) (TransactionIsolationLevel, bool)
	GetSetAutoCommitQuery(autoCommit bool) (string, error)
	GetSetReadOnlyQuery(readOnly bool) (string, error)
	GetSetCatalogQuery(catalog string) (string, error)
	GetSetSchemaQuery(schema string) (string, error)
	GetSetTransactionIsolationQuery(level TransactionIsolationLevel) (string, error)
}

// TopologyDialect provides the SQL queries needed for topology operations.
// This separates query definitions from query execution/parsing.
type TopologyDialect interface {
	DatabaseDialect
	// GetTopologyQuery returns the SQL query to fetch cluster topology.
	GetTopologyQuery() string

	// GetInstanceIdQuery returns the SQL query to get the current instance's identifier.
	GetInstanceIdQuery() string

	// GetWriterIdQuery returns the SQL query to determine if connected to the writer.
	GetWriterIdQuery() string
}

// MultiAzTopologyDialect extends TopologyDialect with Multi-AZ specific methods.
type MultiAzTopologyDialect interface {
	TopologyDialect
	// GetWriterIdColumnName returns the column name for the writer ID in the result set.
	GetWriterIdColumnName() string
}

// GlobalAuroraTopologyDialect extends TopologyDialect with Global Aurora specific methods.
type GlobalAuroraTopologyDialect interface {
	TopologyDialect
	// GetRegionByInstanceIdQuery returns the SQL query to get the region for an instance.
	GetRegionByInstanceIdQuery() string
}

type AuroraLimitlessDialect interface {
	GetLimitlessRouterEndpointQuery() string
	DatabaseDialect
}

type BlueGreenDialect interface {
	GetBlueGreenStatusQuery() string
	IsBlueGreenStatusAvailable(conn driver.Conn) bool
	DatabaseDialect
}
