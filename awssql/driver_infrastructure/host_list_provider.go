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
	"time"
)

type HostListProvider interface {
	Refresh(conn driver.Conn) ([]*host_info_util.HostInfo, error)
	ForceRefresh(conn driver.Conn) ([]*host_info_util.HostInfo, error)
	GetHostRole(conn driver.Conn) host_info_util.HostRole
	IdentifyConnection(conn driver.Conn) (*host_info_util.HostInfo, error)
	GetClusterId() (string, error)
	IsStaticHostListProvider() bool
	CreateHost(hostName string, role host_info_util.HostRole, lag float64, cpu float64, lastUpdateTime time.Time) *host_info_util.HostInfo
}
