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

package connection_tracker

import (
	"database/sql/driver"

	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
)

type ConnectionTracker interface {
	PopulateOpenedConnectionQueue(hostInfo *host_info_util.HostInfo, conn driver.Conn)
	InvalidateAllConnections(hostInfo *host_info_util.HostInfo)
	InvalidateAllConnectionsMultipleHosts(hosts ...string)
	LogOpenedConnections()
	PruneNullConnections()
	ClearCache()
}
