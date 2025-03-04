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
)

type HostListProvider interface {
	Refresh(conn driver.Conn) []HostInfo
	ForceRefresh(conn driver.Conn) []HostInfo
	GetHostRole(conn driver.Conn) HostRole
	IdentifyConnection(conn driver.Conn) HostInfo
	GetClusterId() string
	IsStaticHostListProvider() bool
}
