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
	"awssql/driver_infrastructure"
	"awssql/error_util"
	"database/sql/driver"
	"errors"
)

func ExecuteWithPlugins(
	pluginManager driver_infrastructure.PluginManager,
	methodName string,
	executeFunc driver_infrastructure.ExecuteFunc) (wrappedReturnValue any, wrappedReturnValue2 any, wrappedOk bool, wrappedErr error) {
	return pluginManager.Execute(methodName, executeFunc)
}

func queryWithPlugins(
	pluginManager driver_infrastructure.PluginManager,
	methodName string,
	queryFunc driver_infrastructure.ExecuteFunc,
	engine driver_infrastructure.DatabaseEngine) (driver.Rows, error) {
	result, _, _, err := ExecuteWithPlugins(pluginManager, methodName, queryFunc)

	if err == nil {
		driverRows, ok := result.(driver.Rows)
		if ok {
			rows := AwsWrapperRows{driverRows, pluginManager}
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

func execWithPlugins(pluginManager driver_infrastructure.PluginManager, methodName string, execFunc driver_infrastructure.ExecuteFunc) (driver.Result, error) {
	result, _, _, err := ExecuteWithPlugins(pluginManager, methodName, execFunc)
	if err == nil {
		driverResult, ok := result.(driver.Result)
		if ok {
			return &AwsWrapperResult{driverResult, pluginManager}, nil
		}
		err = errors.New(error_util.GetMessage("AwsWrapperExecuteWithPlugins.unableToCastResult", "driver.Result"))
	}

	return nil, err
}

func prepareWithPlugins(
	pluginManager driver_infrastructure.PluginManager,
	methodName string,
	prepareFunc driver_infrastructure.ExecuteFunc,
	conn AwsWrapperConn) (driver.Stmt, error) {
	result, _, _, err := ExecuteWithPlugins(pluginManager, methodName, prepareFunc)
	if err == nil {
		driverStmt, ok := result.(driver.Stmt)
		if ok {
			return &AwsWrapperStmt{driverStmt, pluginManager, conn}, nil
		}
		err = errors.New(error_util.GetMessage("AwsWrapperExecuteWithPlugins.unableToCastResult", "driver.Stmt"))
	}
	return nil, err
}

func beginWithPlugins(pluginManager driver_infrastructure.PluginManager, methodName string, beginFunc driver_infrastructure.ExecuteFunc) (driver.Tx, error) {
	result, _, _, err := ExecuteWithPlugins(pluginManager, methodName, beginFunc)
	if err == nil {
		driverTx, ok := result.(driver.Tx)
		if ok {
			return &AwsWrapperTx{driverTx, pluginManager}, nil
		}
		err = errors.New(error_util.GetMessage("AwsWrapperExecuteWithPlugins.unableToCastResult", "driver.Tx"))
	}
	return nil, err
}
