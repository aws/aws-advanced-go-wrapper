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

package driver

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
)

func ExecuteWithPlugins(
	connInvokedOn driver.Conn,
	pluginManager driver_infrastructure.PluginManager,
	methodName string,
	executeFunc driver_infrastructure.ExecuteFunc,
	methodArgs ...any) (wrappedReturnValue any, wrappedReturnValue2 any, wrappedOk bool, wrappedErr error) {
	telemetryCtx, ctx := pluginManager.GetTelemetryFactory().OpenTelemetryContext(
		fmt.Sprintf(telemetry.TELEMETRY_EXECUTE, methodName),
		telemetry.TOP_LEVEL,
		nil)
	telemetryCtx.SetAttribute(telemetry.TELEMETRY_ATTRIBUTE_SQL_CALL, methodName)
	pluginManager.SetTelemetryContext(ctx)
	defer func() {
		telemetryCtx.CloseContext()
		pluginManager.SetTelemetryContext(context.TODO())
	}()
	wrappedReturnValue, wrappedReturnValue2, wrappedOk, wrappedErr = pluginManager.Execute(connInvokedOn, methodName, executeFunc, methodArgs...)
	if wrappedErr != nil {
		telemetryCtx.SetSuccess(false)
		telemetryCtx.SetError(wrappedErr)
	} else {
		telemetryCtx.SetSuccess(true)
	}
	return
}

func queryWithPlugins(
	connInvokedOn driver.Conn,
	pluginManager driver_infrastructure.PluginManager,
	methodName string,
	queryFunc driver_infrastructure.ExecuteFunc,
	engine driver_infrastructure.DatabaseEngine,
	methodArgs ...any) (driver.Rows, error) {
	result, _, _, err := ExecuteWithPlugins(connInvokedOn, pluginManager, methodName, queryFunc, methodArgs...)

	if err == nil {
		driverRows, ok := result.(driver.Rows)
		if ok {
			rows := AwsWrapperRows{connInvokedOn, driverRows, pluginManager}
			if engine == driver_infrastructure.MYSQL {
				return &AwsWrapperMySQLRows{rows}, nil
			}
			if engine == driver_infrastructure.PG {
				return &AwsWrapperPgRows{rows}, nil
			}
			return &rows, nil
		}
		err = errors.New(error_util.GetMessage("AwsWrapperExecuteWithPlugins.unableToCastResult", "driver.Rows"))
	}

	return nil, err
}

func execWithPlugins(
	connInvokedOn driver.Conn,
	pluginManager driver_infrastructure.PluginManager,
	methodName string,
	execFunc driver_infrastructure.ExecuteFunc,
	methodArgs ...any) (driver.Result, error) {
	result, _, _, err := ExecuteWithPlugins(connInvokedOn, pluginManager, methodName, execFunc, methodArgs...)
	if err == nil {
		driverResult, ok := result.(driver.Result)
		if ok {
			return &AwsWrapperResult{connInvokedOn, driverResult, pluginManager}, nil
		}
		err = errors.New(error_util.GetMessage("AwsWrapperExecuteWithPlugins.unableToCastResult", "driver.Result"))
	}

	return nil, err
}

func prepareWithPlugins(
	connInvokedOn driver.Conn,
	pluginManager driver_infrastructure.PluginManager,
	methodName string,
	prepareFunc driver_infrastructure.ExecuteFunc,
	conn AwsWrapperConn,
	methodArgs ...any) (driver.Stmt, error) {
	result, _, _, err := ExecuteWithPlugins(connInvokedOn, pluginManager, methodName, prepareFunc, methodArgs...)
	if err == nil {
		driverStmt, ok := result.(driver.Stmt)
		if ok {
			return &AwsWrapperStmt{driverStmt, pluginManager, conn}, nil
		}
		err = errors.New(error_util.GetMessage("AwsWrapperExecuteWithPlugins.unableToCastResult", "driver.Stmt"))
	}
	return nil, err
}

func beginWithPlugins(
	connInvokedOn driver.Conn,
	pluginManager driver_infrastructure.PluginManager,
	pluginService driver_infrastructure.PluginService,
	methodName string,
	beginFunc driver_infrastructure.ExecuteFunc) (driver.Tx, error) {
	result, _, _, err := ExecuteWithPlugins(connInvokedOn, pluginManager, methodName, beginFunc)
	if err == nil {
		driverTx, ok := result.(driver.Tx)
		if ok {
			pluginService.SetCurrentTx(driverTx)
			return &AwsWrapperTx{connInvokedOn, driverTx, pluginManager, pluginService}, nil
		}
		err = errors.New(error_util.GetMessage("AwsWrapperExecuteWithPlugins.unableToCastResult", "driver.Tx"))
	}
	return nil, err
}
