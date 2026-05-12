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

package bun_pg_driver

import (
	"database/sql"
	"database/sql/driver"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils"
	"github.com/uptrace/bun/driver/pgdriver"
)

var bunPgPersistingProperties = []string{
	property_util.USER.Name,
	property_util.PASSWORD.Name,
	property_util.HOST.Name,
	property_util.DATABASE.Name,
	property_util.PORT.Name,
}

type BunPgDriverDialect struct {
	errorHandler error_util.ErrorHandler
}

const (
	BUN_PG_DRIVER_CLASS_NAME        = "pgdriver.Driver"
	BUN_PG_DRIVER_REGISTRATION_NAME = "bunpg"
)

func NewBunPgDriverDialect() *BunPgDriverDialect {
	return &BunPgDriverDialect{errorHandler: &BunPgErrorHandler{}}
}

func (d BunPgDriverDialect) IsDialect(drv driver.Driver) bool {
	typeName := reflect.TypeOf(drv).String()
	return typeName == BUN_PG_DRIVER_CLASS_NAME || typeName == "*"+BUN_PG_DRIVER_CLASS_NAME
}

func (d BunPgDriverDialect) GetAllowedOnConnectionMethodNames() []string {
	return append(utils.REQUIRED_METHODS, utils.ROWS_COLUMN_TYPE_LENGTH)
}

func (d BunPgDriverDialect) IsNetworkError(err error) bool {
	return d.errorHandler.IsNetworkError(err)
}

func (d BunPgDriverDialect) IsLoginError(err error) bool {
	return d.errorHandler.IsLoginError(err)
}

func (d BunPgDriverDialect) IsClosed(conn driver.Conn) bool {
	if validator, ok := conn.(driver.Validator); ok {
		return !validator.IsValid()
	}
	return false
}

func (d BunPgDriverDialect) IsDriverRegistered(drivers map[string]driver.Driver) bool {
	_, exists := drivers[BUN_PG_DRIVER_REGISTRATION_NAME]
	return exists
}

func (d BunPgDriverDialect) RegisterDriver() {
	for _, name := range sql.Drivers() {
		if name == BUN_PG_DRIVER_REGISTRATION_NAME {
			return
		}
	}
	sql.Register(BUN_PG_DRIVER_REGISTRATION_NAME, pgdriver.NewDriver())
}

func (d BunPgDriverDialect) GetDriverRegistrationName() string {
	return BUN_PG_DRIVER_REGISTRATION_NAME
}

// PrepareDsn builds a PostgreSQL URL-format DSN from the wrapper's properties
// map, applying host/port overrides from the HostInfo (which the wrapper sets
// during failover to point to the new primary).
//
// bun's pgdriver requires URL-format DSN (postgres://user@host:port/db?params),
// unlike pgx which uses key=value format. The AWS wrapper calls this on every
// reconnection (including failover).
func (d BunPgDriverDialect) PrepareDsn(properties map[string]string, hostInfo *host_info_util.HostInfo) string {
	copyProps := property_util.RemoveInternalAwsWrapperProperties(properties)

	host := copyProps[property_util.HOST.Name]
	port := copyProps[property_util.PORT.Name]
	user := copyProps[property_util.USER.Name]
	password := copyProps[property_util.PASSWORD.Name]
	database := copyProps[property_util.DATABASE.Name]

	if !hostInfo.IsNil() {
		host = hostInfo.Host
		if hostInfo.Port != host_info_util.HOST_NO_PORT {
			port = strconv.Itoa(hostInfo.Port)
		}
	}
	if port == "" {
		port = "5432"
	}

	var dsn strings.Builder
	dsn.WriteString("postgres://")
	if password != "" {
		dsn.WriteString(url.UserPassword(user, password).String())
	} else {
		dsn.WriteString(url.User(user).String())
	}
	dsn.WriteString("@")
	dsn.WriteString(host)
	dsn.WriteString(":")
	dsn.WriteString(port)
	dsn.WriteString("/")
	dsn.WriteString(url.PathEscape(database))

	query := url.Values{}
	coreProps := map[string]bool{
		property_util.USER.Name:     true,
		property_util.PASSWORD.Name: true,
		property_util.HOST.Name:     true,
		property_util.PORT.Name:     true,
		property_util.DATABASE.Name: true,
	}
	for k, v := range copyProps {
		if coreProps[k] {
			continue
		}
		if slices.Contains(bunPgPersistingProperties, k) || !property_util.ALL_WRAPPER_PROPERTIES[k] {
			query.Set(k, v)
		}
	}
	if len(query) > 0 {
		dsn.WriteString("?")
		dsn.WriteString(query.Encode())
	}

	return dsn.String()
}

func (d BunPgDriverDialect) GetRowParser() driver_infrastructure.RowParser {
	return defaultRowParser
}

func (d BunPgDriverDialect) GetPropertyResolver() driver_infrastructure.DriverPropertyResolver {
	return defaultPropertyResolver
}
