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
	"awssql/property_util"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/stdlib"
)

var pgxPersistingProperties = []string{
	property_util.USER.Name,
	property_util.PASSWORD.Name,
	property_util.HOST.Name,
	property_util.DATABASE.Name,
	property_util.PORT.Name,
}

type PgxDriverDialect struct {
	errorHandler error_util.ErrorHandler
}

const (
	PGX_DRIVER_CLASS_NAME           = "stdlib.Driver"
	PGX_DRIVER_REGISTRATION_NAME    = "pgx"
	PGX_V5_DRIVER_REGISTRATION_NAME = "pgx/v5"
)

func NewPgxDriverDialect() *PgxDriverDialect {
	return &PgxDriverDialect{errorHandler: &PgxErrorHandler{}}
}

func (p PgxDriverDialect) IsDialect(driver driver.Driver) bool {
	return PGX_DRIVER_CLASS_NAME == reflect.TypeOf(driver).String() || "*"+PGX_DRIVER_CLASS_NAME == reflect.TypeOf(driver).String()
}

func (p PgxDriverDialect) GetAllowedOnConnectionMethodNames() []string {
	return append(REQUIRED_METHODS, ROWS_COLUMN_TYPE_LENGTH)
}

func (p PgxDriverDialect) IsNetworkError(err error) bool {
	return p.errorHandler.IsNetworkError(err)
}

func (p PgxDriverDialect) IsLoginError(err error) bool {
	return p.errorHandler.IsLoginError(err)
}

func (p PgxDriverDialect) IsDriverRegistered(drivers map[string]driver.Driver) bool {
	_, existsV4 := drivers[PGX_DRIVER_REGISTRATION_NAME]
	_, existsV5 := drivers[PGX_V5_DRIVER_REGISTRATION_NAME]
	return existsV4 || existsV5
}

func (p PgxDriverDialect) RegisterDriver() {
	sql.Register(PGX_DRIVER_REGISTRATION_NAME, &stdlib.Driver{})
}

func (p PgxDriverDialect) PrepareDsn(properties map[string]string, hostInfo *host_info_util.HostInfo) string {
	var builder strings.Builder
	copyProps := property_util.RemoveMonitoringProperties(properties)
	for k, v := range copyProps {
		if slices.Contains(pgxPersistingProperties, k) || !property_util.ALL_WRAPPER_PROPERTIES[k] {
			value := v
			if builder.Len() != 0 {
				builder.WriteString(" ")
			}
			if k == property_util.PORT.Name && !hostInfo.IsNil() && hostInfo.Port != host_info_util.HOST_NO_PORT {
				value = strconv.Itoa(hostInfo.Port)
			}
			if k == property_util.HOST.Name && !hostInfo.IsNil() {
				value = hostInfo.Host
			}
			builder.WriteString(fmt.Sprintf("%s=%s", k, value))
		}
	}
	return builder.String()
}
