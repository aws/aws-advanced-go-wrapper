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

package mysql_driver_2

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

type MySQL2DriverDialect struct {
	errorHandler error_util.ErrorHandler
}

const (
	MYSQL_DRIVER_REGISTRATION_NAME = "mysql"
)

func NewMySQL2DriverDialect() *MySQL2DriverDialect {
	return &MySQL2DriverDialect{errorHandler: MySQL2ErrorHandler{}}
}

func (m MySQL2DriverDialect) GetAllowedOnConnectionMethodNames() []string {
	return append(utils.REQUIRED_METHODS, utils.ROWS_HAS_NEXT_RESULT_SET, utils.ROWS_NEXT_RESULT_SET, utils.ROWS_COLUMN_TYPE_SCAN_TYPE, utils.ROWS_COLUMN_TYPE_NULLABLE)
}

func (m MySQL2DriverDialect) IsNetworkError(err error) bool {
	return m.errorHandler.IsNetworkError(err)
}

func (m MySQL2DriverDialect) IsLoginError(err error) bool {
	return m.errorHandler.IsLoginError(err)
}

func (m MySQL2DriverDialect) IsClosed(conn driver.Conn) bool {
	validator, ok := conn.(driver.Validator)
	if ok {
		return !validator.IsValid()
	}
	// This should not be reached.
	return false
}

func (m MySQL2DriverDialect) IsDriverRegistered(drivers map[string]driver.Driver) bool {
	_, exists := drivers[MYSQL_DRIVER_REGISTRATION_NAME]
	return exists
}

func (m MySQL2DriverDialect) RegisterDriver() {
	sql.Register(MYSQL_DRIVER_REGISTRATION_NAME, MySqlUnderlyingDriver{})
}

func (m MySQL2DriverDialect) PrepareDsn(properties map[string]string, hostInfo *host_info_util.HostInfo) string {
	var builder strings.Builder

	username := properties[property_util.USER.Name]
	password := properties[property_util.PASSWORD.Name]
	address := properties[property_util.HOST.Name]
	database := properties[property_util.DATABASE.Name]
	port := properties[property_util.PORT.Name]

	// Build MySQL2 format: [user[:password]@]addr[/db[?param=X]]
	if username != "" {
		builder.WriteString(username)
		// For IAM tokens containing host info, don't put in password field
		if password != "" {
			if !strings.Contains(password, "Action=connect") {
				fmt.Println("not encoding password: ", password)
				builder.WriteString(":" + password)
			} else {
				fmt.Println("encoding password: ", password)
				builder.WriteString(":" + url.QueryEscape(password))
			}
		}
		builder.WriteString("@")
	}

	if address != "" {
		if !hostInfo.IsNil() {
			address = hostInfo.Host
		}
		builder.WriteString(address)

		if !hostInfo.IsNil() && hostInfo.Port != host_info_util.HOST_NO_PORT {
			builder.WriteString(":" + strconv.Itoa(hostInfo.Port))
		} else if port != "" {
			builder.WriteString(":" + port)
		}
	}

	if database != "" {
		builder.WriteString("/" + url.PathEscape(database))
	}

	var params strings.Builder
	copyProps := property_util.RemoveInternalAwsWrapperProperties(properties)
	for k, v := range copyProps {
		if !property_util.ALL_WRAPPER_PROPERTIES[k] {
			if params.Len() != 0 {
				params.WriteString("&")
			}
			params.WriteString(fmt.Sprintf("%s=%s", url.QueryEscape(k), url.QueryEscape(v)))
		}
	}

	if params.Len() != 0 {
		builder.WriteString("?" + params.String())
	}
	result := builder.String()
	fmt.Println("RETURNED DSN: ", result)
	return result
}

func (m MySQL2DriverDialect) GetDriverRegistrationName() string {
	return MYSQL_DRIVER_REGISTRATION_NAME
}
