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

package container

import (
	"awssql/driver_infrastructure"
	"awssql/plugin_helpers"
	"database/sql/driver"
)

type Container struct {
	PluginManager *driver_infrastructure.PluginManager
	PluginService *driver_infrastructure.PluginService
}

func NewContainer(dsn string, targetDriver driver.Driver) (Container, error) {
	props := driver_infrastructure.PropertiesFromDsn(dsn)
	defaultConnProvider := driver_infrastructure.ConnectionProvider(driver_infrastructure.NewDriverConnectionProvider(targetDriver))

	pluginManager := driver_infrastructure.PluginManager(plugin_helpers.NewPluginManagerImpl(targetDriver, &defaultConnProvider, nil, props))
	pluginServiceImpl := driver_infrastructure.PluginService(plugin_helpers.NewPluginServiceImpl(&pluginManager, props))

	pluginChainBuilder := ConnectionPluginChainBuilder{}
	plugins, _ := pluginChainBuilder.GetPlugins(&pluginServiceImpl, &pluginManager, props)

	err := pluginManager.Init(&pluginServiceImpl, props, plugins)
	if err != nil {
		return Container{}, err
	}

	return Container{
		PluginManager: &pluginManager,
		PluginService: &pluginServiceImpl,
	}, nil
}
