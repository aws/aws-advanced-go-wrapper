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
	"awssql/host_info_util"
	"database/sql/driver"
)

type DatabaseDialect interface {
	GetDefaultPort() int
	GetHostAliasQuery() string
	GetServerVersionQuery() string
	GetDialectUpdateCandidates() []string
	IsDialect(conn driver.Conn) bool
	GetHostListProvider(props map[string]string, initialDsn string, hostListProviderService HostListProviderService, pluginService PluginService) HostListProvider
}

type TopologyAwareDialect interface {
	GetTopology(conn driver.Conn, provider HostListProvider) ([]*host_info_util.HostInfo, error)
	GetHostRole(conn driver.Conn) host_info_util.HostRole
	GetHostName(conn driver.Conn) string
	GetWriterHostName(conn driver.Conn) (string, error)
	DatabaseDialect
}

type AuroraLimitlessDialect interface {
	GetLimitlessRouterEndpointQuery() string
	DatabaseDialect
}
