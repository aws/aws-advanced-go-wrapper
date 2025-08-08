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

package mysql_driver

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"

	"github.com/go-sql-driver/mysql"
)

type MySQLDriverDialect struct {
	errorHandler error_util.ErrorHandler
}

const (
	MYSQL_DRIVER_CLASS_NAME        = "mysql.MySQLDriver"
	MYSQL_DRIVER_REGISTRATION_NAME = "mysql"
)

func NewMySQLDriverDialect() *MySQLDriverDialect {
	return &MySQLDriverDialect{errorHandler: MySQLErrorHandler{}}
}

func (m MySQLDriverDialect) IsDialect(driver driver.Driver) bool {
	return MYSQL_DRIVER_CLASS_NAME == reflect.TypeOf(driver).String() || "*"+MYSQL_DRIVER_CLASS_NAME == reflect.TypeOf(driver).String()
}

func (m MySQLDriverDialect) GetAllowedOnConnectionMethodNames() []string {
	return append(utils.REQUIRED_METHODS, utils.ROWS_HAS_NEXT_RESULT_SET, utils.ROWS_NEXT_RESULT_SET, utils.ROWS_COLUMN_TYPE_SCAN_TYPE, utils.ROWS_COLUMN_TYPE_NULLABLE)
}

func (m MySQLDriverDialect) IsNetworkError(err error) bool {
	return m.errorHandler.IsNetworkError(err)
}

func (m MySQLDriverDialect) IsLoginError(err error) bool {
	return m.errorHandler.IsLoginError(err)
}

func (m MySQLDriverDialect) IsClosed(conn driver.Conn) bool {
	validator, ok := conn.(driver.Validator)
	if ok {
		return !validator.IsValid()
	}
	// This should not be reached.
	return false
}

func (m MySQLDriverDialect) IsDriverRegistered(drivers map[string]driver.Driver) bool {
	_, exists := drivers[MYSQL_DRIVER_REGISTRATION_NAME]
	return exists
}

func (m MySQLDriverDialect) RegisterDriver() {
	sql.Register(MYSQL_DRIVER_REGISTRATION_NAME, &mysql.MySQLDriver{})
}

func (m MySQLDriverDialect) PrepareDsn(properties map[string]string, hostInfo *host_info_util.HostInfo) string {
	var builder strings.Builder

	username := properties[property_util.USER.Name]
	password := properties[property_util.PASSWORD.Name]
	address := properties[property_util.HOST.Name]
	database := properties[property_util.DATABASE.Name]
	net := properties[property_util.NET.Name]
	port := properties[property_util.PORT.Name]

	if username != "" {
		if password != "" {
			password = ":" + password
		}
		builder.WriteString(fmt.Sprintf("%s%s@", username, password))
	}

	if net != "" {
		builder.WriteString(net)
	}

	if address != "" {
		if !hostInfo.IsNil() && hostInfo.Port != host_info_util.HOST_NO_PORT {
			port = ":" + strconv.Itoa(hostInfo.Port)
		} else if port != "" {
			port = ":" + port
		}

		if !hostInfo.IsNil() {
			address = hostInfo.Host
		}
		builder.WriteString(fmt.Sprintf("(%s%s)", address, port))
	}

	builder.WriteString("/")

	if database != "" {
		builder.WriteString(url.PathEscape(database))
	}

	var params strings.Builder
	copyProps := property_util.RemoveInternalAwsWrapperProperties(properties)
	for k, v := range copyProps {
		if !property_util.ALL_WRAPPER_PROPERTIES[k] {
			if params.Len() != 0 {
				params.WriteString("&")
			}
			params.WriteString(fmt.Sprintf("%s=%s", k, v))
		}
	}

	if params.Len() != 0 {
		builder.WriteString(fmt.Sprintf("?%s", params.String()))
	}
	return builder.String()
}
