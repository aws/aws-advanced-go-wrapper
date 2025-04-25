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

const (
	CONN_PREPARE                        = "Conn.Prepare"
	CONN_PREPARE_CONTEXT                = "Conn.PrepareContext"
	CONN_CLOSE                          = "Conn.Close"
	CONN_BEGIN                          = "Conn.Begin"
	CONN_BEGIN_TX                       = "Conn.BeginTx"
	CONN_QUERY_CONTEXT                  = "Conn.QueryContext"
	CONN_EXEC_CONTEXT                   = "Conn.ExecContext"
	CONN_PING                           = "Conn.Ping"
	CONN_IS_VALID                       = "Conn.IsValid"
	CONN_RESET_SESSION                  = "Conn.ResetSession"
	CONN_CHECK_NAMED_VALUE              = "Conn.CheckNamedValue"
	STMT_CLOSE                          = "Stmt.Close"
	STMT_EXEC                           = "Stmt.Exec"
	STMT_EXEC_CONTEXT                   = "Stmt.ExecContext"
	STMT_NUM_INPUT                      = "Stmt.NumInput"
	STMT_QUERY                          = "Stmt.Query"
	STMT_QUERY_CONTEXT                  = "Stmt.QueryContext"
	STMT_CHECK_NAMED_VALUE              = "Stmt.CheckNamedValue"
	RESULT_LAST_INSERT_ID               = "Result.LastInsertId"
	RESULT_ROWS_AFFECTED                = "Result.RowsAffected"
	TX_COMMIT                           = "Tx.Commit"
	TX_ROLLBACK                         = "Tx.Rollback"
	ROWS_CLOSE                          = "Rows.Close"
	ROWS_COLUMNS                        = "Rows.Columns"
	ROWS_NEXT                           = "Rows.Next"
	ROWS_COLUMN_TYPE_PRECISION_SCALE    = "Rows.ColumnTypePrecisionScale"
	ROWS_COLUMN_TYPE_DATABASE_TYPE_NAME = "Rows.ColumnTypeDatabaseTypeName"
	ROWS_COLUMN_TYPE_LENGTH             = "Rows.ColumnTypeLength"
	ROWS_HAS_NEXT_RESULT_SET            = "Rows.HasNextResultSet"
	ROWS_NEXT_RESULT_SET                = "Rows.NextResultSet"
	ROWS_COLUMN_TYPE_SCAN_TYPE          = "Rows.ColumnTypeScanType"
	ROWS_COLUMN_TYPE_NULLABLE           = "Rows.ColumnTypeNullable"
)

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

var NETWORK_BOUND_METHODS = []string{
	CONN_PREPARE,
	CONN_PREPARE_CONTEXT,
	CONN_QUERY_CONTEXT,
	CONN_EXEC_CONTEXT,
	CONN_PING,
	CONN_IS_VALID,
	CONN_RESET_SESSION,
	STMT_EXEC,
	STMT_EXEC_CONTEXT,
	STMT_QUERY,
	STMT_QUERY_CONTEXT,
	TX_COMMIT,
	TX_ROLLBACK,
	ROWS_NEXT,
	ROWS_HAS_NEXT_RESULT_SET,
	ROWS_NEXT_RESULT_SET,
}
