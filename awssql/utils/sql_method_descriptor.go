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

package utils

// SqlMethodDescriptor defines execution behavior for a driver method.
type SqlMethodDescriptor struct {
	// CheckBoundedConnection controls whether PluginManager.Execute validates
	// that connInvokedOn matches the current connection. Set to false for methods
	// that read locally-buffered data or are resource cleanup operations.
	CheckBoundedConnection bool

	// UsePipeline controls whether the method is routed through the plugin chain
	// and telemetry. When false, ExecuteWithPlugins calls the executeFunc directly,
	// bypassing all plugins and telemetry. Changing this to true re-engages the
	// full pipeline with no other code changes required.
	UsePipeline bool
}

// SqlMethods maps each driver method name to its execution descriptor.
var SqlMethods = map[string]SqlMethodDescriptor{
	CONN_PREPARE:                        {CheckBoundedConnection: true, UsePipeline: true},
	CONN_PREPARE_CONTEXT:                {CheckBoundedConnection: true, UsePipeline: true},
	CONN_CLOSE:                          {CheckBoundedConnection: false, UsePipeline: true},
	CONN_BEGIN:                          {CheckBoundedConnection: true, UsePipeline: true},
	CONN_BEGIN_TX:                       {CheckBoundedConnection: true, UsePipeline: true},
	CONN_QUERY_CONTEXT:                  {CheckBoundedConnection: true, UsePipeline: true},
	CONN_EXEC_CONTEXT:                   {CheckBoundedConnection: true, UsePipeline: true},
	CONN_PING:                           {CheckBoundedConnection: true, UsePipeline: true},
	CONN_IS_VALID:                       {CheckBoundedConnection: false, UsePipeline: true},
	CONN_RESET_SESSION:                  {CheckBoundedConnection: true, UsePipeline: true},
	CONN_CHECK_NAMED_VALUE:              {CheckBoundedConnection: false, UsePipeline: false},
	STMT_CLOSE:                          {CheckBoundedConnection: false, UsePipeline: true},
	STMT_EXEC:                           {CheckBoundedConnection: true, UsePipeline: true},
	STMT_EXEC_CONTEXT:                   {CheckBoundedConnection: true, UsePipeline: true},
	STMT_NUM_INPUT:                      {CheckBoundedConnection: false, UsePipeline: false},
	STMT_QUERY:                          {CheckBoundedConnection: true, UsePipeline: true},
	STMT_QUERY_CONTEXT:                  {CheckBoundedConnection: true, UsePipeline: true},
	STMT_CHECK_NAMED_VALUE:              {CheckBoundedConnection: false, UsePipeline: false},
	RESULT_LAST_INSERT_ID:               {CheckBoundedConnection: false, UsePipeline: false},
	RESULT_ROWS_AFFECTED:                {CheckBoundedConnection: false, UsePipeline: false},
	TX_COMMIT:                           {CheckBoundedConnection: true, UsePipeline: true},
	TX_ROLLBACK:                         {CheckBoundedConnection: true, UsePipeline: true},
	ROWS_CLOSE:                          {CheckBoundedConnection: false, UsePipeline: true},
	ROWS_COLUMNS:                        {CheckBoundedConnection: false, UsePipeline: false},
	ROWS_NEXT:                           {CheckBoundedConnection: true, UsePipeline: true},
	ROWS_COLUMN_TYPE_PRECISION_SCALE:    {CheckBoundedConnection: false, UsePipeline: false},
	ROWS_COLUMN_TYPE_DATABASE_TYPE_NAME: {CheckBoundedConnection: false, UsePipeline: false},
	ROWS_COLUMN_TYPE_LENGTH:             {CheckBoundedConnection: false, UsePipeline: false},
	ROWS_HAS_NEXT_RESULT_SET:            {CheckBoundedConnection: true, UsePipeline: true},
	ROWS_NEXT_RESULT_SET:                {CheckBoundedConnection: true, UsePipeline: true},
	ROWS_COLUMN_TYPE_SCAN_TYPE:          {CheckBoundedConnection: false, UsePipeline: false},
	ROWS_COLUMN_TYPE_NULLABLE:           {CheckBoundedConnection: false, UsePipeline: false},
}
