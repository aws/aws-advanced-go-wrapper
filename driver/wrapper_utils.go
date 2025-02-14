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
	"database/sql/driver"
	"errors"
)

func executeWithPlugins(pluginManager PluginManager, methodName string, executeFunc ExecuteFunc) (any, error) {
	return pluginManager.ExecuteWithSubscribedPlugins(methodName, executeFunc)
}

func queryWithPlugins(pluginManager PluginManager, methodName string, queryFunc ExecuteFunc) (driver.Rows, error) {
	result, err := executeWithPlugins(pluginManager, methodName, queryFunc)
	if err == nil {
		driverRows, ok := result.(driver.Rows)
		if ok {
			return driverRows, nil
		}
		err = errors.New(GetMessage("AwsWrapperExecuteWithPlugins.unableToCastResult", "driver.Rows"))
	}

	return nil, err
}

func execWithPlugins(pluginManager PluginManager, methodName string, execFunc ExecuteFunc) (driver.Result, error) {
	result, err := executeWithPlugins(pluginManager, methodName, execFunc)
	if err == nil {
		driverResult, ok := result.(driver.Result)
		if ok {
			return driverResult, nil
		}
		err = errors.New(GetMessage("AwsWrapperExecuteWithPlugins.unableToCastResult", "driver.Result"))
	}

	return nil, err
}

func prepareWithPlugins(pluginManager PluginManager, methodName string, prepareFunc ExecuteFunc) (driver.Stmt, error) {
	result, err := executeWithPlugins(pluginManager, methodName, prepareFunc)
	if err == nil {
		driverStmt, ok := result.(driver.Stmt)
		if ok {
			return &AwsWrapperStmt{underlyingStmt: driverStmt, pluginManager: pluginManager}, nil
		}
		err = errors.New(GetMessage("AwsWrapperExecuteWithPlugins.unableToCastResult", "driver.Stmt"))
	}
	return nil, err
}

func beginWithPlugins(pluginManager PluginManager, methodName string, beginFunc ExecuteFunc) (driver.Tx, error) {
	result, err := executeWithPlugins(pluginManager, methodName, beginFunc)
	if err == nil {
		driverTx, ok := result.(driver.Tx)
		if ok {
			return driverTx, nil
		}
		err = errors.New(GetMessage("AwsWrapperExecuteWithPlugins.unableToCastResult", "driver.Tx"))
	}
	return nil, err
}
