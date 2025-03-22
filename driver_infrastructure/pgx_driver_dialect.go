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
	"awssql/property_util"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/jackc/pgx/v5/stdlib"
	"reflect"
	"slices"
	"strings"
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
	for driverName, _ := range drivers {
		if driverName == PGX_DRIVER_REGISTRATION_NAME || driverName == PGX_V5_DRIVER_REGISTRATION_NAME {
			return true
		}
	}
	return false
}

func (p PgxDriverDialect) RegisterDriver() {
	sql.Register(PGX_DRIVER_REGISTRATION_NAME, &stdlib.Driver{})
}

func (p PgxDriverDialect) PrepareDsn(properties map[string]string) string {
	var builder strings.Builder
	for k, v := range properties {
		if slices.Contains(pgxPersistingProperties, k) || !property_util.ALL_WRAPPER_PROPERTIES[k] {
			if builder.Len() != 0 {
				builder.WriteString(" ")
			}
			builder.WriteString(fmt.Sprintf("%s=%s", k, v))
		}
	}
	return builder.String()
}
