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
)

type ConnectFunc func() (driver.Conn, error)
type ExecuteFunc func() (any, any, bool, error)

type PluginManager interface {
	Init()
	ExecuteWithSubscribedPlugins(methodName string, driverFunc ExecuteFunc) (wrappedReturnValue any, wrappedReturnValue2 any, wrappedOk bool, wrappedErr error)
}

type ConnectionPluginManager struct {
	targetDriver driver.Driver
	plugins      []ConnectionPlugin
}

func (pluginManager *ConnectionPluginManager) Init() {
	// TODO: initialize plugins list
	pluginManager.plugins = []ConnectionPlugin{}
}

func (pluginManager *ConnectionPluginManager) ExecuteWithSubscribedPlugins(methodName string, executeFunc ExecuteFunc) (wrappedReturnValue any, wrappedReturnValue2 any, wrappedOk bool, wrappedErr error) {
	pluginChain := executeFunc
	for _, plugin := range pluginManager.plugins {
		oldPluginChain := pluginChain
		pluginChain = func() (any, any, bool, error) {
			return plugin.Execute(methodName, oldPluginChain)
		}
	}
	return pluginChain()
}
