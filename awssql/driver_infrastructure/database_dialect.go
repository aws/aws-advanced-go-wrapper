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

	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

type DatabaseDialect interface {
	GetDefaultPort() int
	GetHostAliasQuery() string
	GetServerVersionQuery() string
	GetDialectUpdateCandidates() []string
	IsDialect(conn driver.Conn) bool
	GetHostListProvider(props *utils.RWMap[string, string], hostListProviderService HostListProviderService, pluginService PluginService) HostListProvider
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

type TopologyAwareDialect interface {
	GetTopology(conn driver.Conn, provider HostListProvider) ([]*host_info_util.HostInfo, error)
	GetHostRole(conn driver.Conn) host_info_util.HostRole
	GetHostName(conn driver.Conn) (string, string)
	GetWriterHostName(conn driver.Conn) (string, error)
	DatabaseDialect
}

type AuroraLimitlessDialect interface {
	GetLimitlessRouterEndpointQuery() string
	DatabaseDialect
}

type BlueGreenDialect interface {
	GetBlueGreenStatus(conn driver.Conn) []BlueGreenResult
	IsBlueGreenStatusAvailable(conn driver.Conn) bool
	DatabaseDialect
}
