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

package plugins

import (
	"database/sql/driver"
	"log/slog"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
)

type ExecutionTimePluginFactory struct{}

func NewExecutionTimePluginFactory() driver_infrastructure.ConnectionPluginFactory {
	return ExecutionTimePluginFactory{}
}

func (factory ExecutionTimePluginFactory) GetInstance(pluginService driver_infrastructure.PluginService,
	props map[string]string,
) (driver_infrastructure.ConnectionPlugin, error) {
	return NewExecutionTimePlugin(pluginService, props)
}

func (factory ExecutionTimePluginFactory) ClearCaches() {}

type ExecutionTimePlugin struct {
	BaseConnectionPlugin
	executionTime int64
}

func NewExecutionTimePlugin(pluginService driver_infrastructure.PluginService,
	props map[string]string) (*ExecutionTimePlugin, error) {
	return &ExecutionTimePlugin{}, nil
}

func (d *ExecutionTimePlugin) GetSubscribedMethods() []string {
	return []string{plugin_helpers.ALL_METHODS}
}

func (d *ExecutionTimePlugin) Execute(
	connInvokedOn driver.Conn,
	methodName string,
	executeFunc driver_infrastructure.ExecuteFunc,
	methodArgs ...any) (wrappedReturnValue any, wrappedReturnValue2 any, wrappedOk bool, wrappedErr error) {
	start := time.Now()
	wrappedReturnValue, wrappedReturnValue2, wrappedOk, wrappedErr = executeFunc()
	elapsed := time.Since(start)

	d.executionTime += elapsed.Nanoseconds()
	slog.Debug(error_util.GetMessage("ExecutionTimePlugin.executionTime", methodName, elapsed.Milliseconds()))
	return
}

func (d *ExecutionTimePlugin) ResetExecutionTime() {
	d.executionTime = 0
}

func (d *ExecutionTimePlugin) GetTotalExecutionTime() int64 {
	return d.executionTime
}
