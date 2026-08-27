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
	"log/slog"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils"
)

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
	switch drv.(type) {
	case BunPgUnderlyingDriver, *BunPgUnderlyingDriver:
		return true
	}
	typeName := reflect.TypeOf(drv).String()
	return typeName == BUN_PG_DRIVER_CLASS_NAME || typeName == "*"+BUN_PG_DRIVER_CLASS_NAME
}

func (d BunPgDriverDialect) GetAllowedOnConnectionMethodNames() []string {
	return utils.REQUIRED_METHODS
}

func (d BunPgDriverDialect) IsNetworkError(err error) bool {
	return d.errorHandler.IsNetworkError(err)
}

func (d BunPgDriverDialect) IsLoginError(err error) bool {
	return d.errorHandler.IsLoginError(err)
}

func (d BunPgDriverDialect) IsReadOnlyError(err error) bool {
	return d.errorHandler.IsReadOnlyError(err)
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
	sql.Register(BUN_PG_DRIVER_REGISTRATION_NAME, NewUnderlyingDriver())
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
	if user == "" {
		// Emit no userinfo at all rather than an empty user. pgdriver's WithUser panics
		// on an empty string, and every form that carries userinfo reaches it: even
		// "postgres://@host" parses to a non-nil, empty url.Userinfo. Omitting the
		// section leaves u.User nil, so pgdriver falls back to its own default user and
		// the connection fails authentication - an error the caller can handle, rather
		// than a panic that takes the process down. Any password is dropped with it,
		// which costs nothing: a connection with no user cannot authenticate anyway.
		//
		// host and database need no such guard; pgdriver only applies those when the
		// parsed URL actually carries them.
		slog.Warn(error_util.GetMessage("BunPgDriverDialect.noUserInProperties"))
	} else {
		if password != "" {
			dsn.WriteString(url.UserPassword(user, password).String())
		} else {
			dsn.WriteString(url.User(user).String())
		}
		dsn.WriteString("@")
	}
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
		// Only forward properties the wrapper does not own. The five connection-identity keys
		// are already in the URL itself and are skipped by coreProps above.
		if !property_util.ALL_WRAPPER_PROPERTIES[k] {
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
