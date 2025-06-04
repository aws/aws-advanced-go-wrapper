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
	"errors"
	"strings"

	"github.com/go-sql-driver/mysql"
)

const SqlStateAccessError = "28000"

var MySqlNetworkErrorMessages = []string{
	"invalid connection",
	"bad connection",
	"broken pipe",
}

type MySQLErrorHandler struct {
}

func (m MySQLErrorHandler) IsNetworkError(err error) bool {
	sqlState := m.getSQLStateFromError(err)
	if sqlState != "" && string(sqlState[0:2]) == "08" {
		return true
	}

	for _, networkError := range MySqlNetworkErrorMessages {
		if strings.Contains(err.Error(), networkError) {
			return true
		}
	}

	return false
}

func (m MySQLErrorHandler) IsLoginError(err error) bool {
	sqlState := m.getSQLStateFromError(err)
	return sqlState == SqlStateAccessError
}

func (m MySQLErrorHandler) getSQLStateFromError(err error) string {
	var mysqlErr *mysql.MySQLError
	ok := errors.As(err, &mysqlErr)
	if ok {
		return string(mysqlErr.SQLState[:])
	}
	return ""
}
