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

	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
)

type DriverDialect interface {
	IsDialect(driver driver.Driver) bool
	GetAllowedOnConnectionMethodNames() []string
	PrepareDsn(properties map[string]string, info *host_info_util.HostInfo) string
	IsNetworkError(err error) bool
	IsLoginError(err error) bool
	IsClosed(conn driver.Conn) bool
	IsDriverRegistered(drivers map[string]driver.Driver) bool
	RegisterDriver()
	GetDriverRegistrationName() string
	GetRowParser() RowParser
}

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
