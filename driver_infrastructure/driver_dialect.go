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

import "database/sql/driver"

var REQUIRED_METHODS = []string{
	CONN_PREPARE,
	CONN_PREPARE_CONTEXT,
	CONN_CLOSE,
	CONN_BEGIN,
	CONN_BEGIN_TX,
	CONN_QUERY_CONTEXT,
	CONN_EXEC_CONTEXT,
	CONN_PING,
	CONN_IS_VALID,
	CONN_RESET_SESSION,
	CONN_CHECK_NAMED_VALUE,
	STMT_CLOSE,
	STMT_EXEC,
	STMT_EXEC_CONTEXT,
	STMT_NUM_INPUT,
	STMT_QUERY,
	STMT_QUERY_CONTEXT,
	STMT_CHECK_NAMED_VALUE,
	RESULT_LAST_INSERT_ID,
	RESULT_ROWS_AFFECTED,
	TX_COMMIT,
	TX_ROLLBACK,
	ROWS_CLOSE,
	ROWS_COLUMNS,
	ROWS_NEXT,
	ROWS_COLUMN_TYPE_PRECISION_SCALE,
	ROWS_COLUMN_TYPE_DATABASE_TYPE_NAME,
}

type DriverDialect interface {
	IsDialect(driver driver.Driver) bool
	GetAllowedOnConnectionMethodNames() []string
	PrepareDsn(properties map[string]string) string
	IsNetworkError(err error) bool
	IsLoginError(err error) bool
	IsDriverRegistered(drivers map[string]driver.Driver) bool
	RegisterDriver()
}
