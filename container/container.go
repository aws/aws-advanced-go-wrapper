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
	"awssql/utils"
	"database/sql/driver"
)

type Container struct {
	PluginManager *driver_infrastructure.PluginManager
	PluginService *driver_infrastructure.PluginService
}

func NewContainer(dsn string, targetDriver driver.Driver) (Container, error) {
	props, parseErr := utils.ParseDsn(dsn)
	if parseErr != nil {
		return Container{}, parseErr
	}
	defaultConnProvider := driver_infrastructure.ConnectionProvider(driver_infrastructure.NewDriverConnectionProvider(targetDriver))

	pluginManager := driver_infrastructure.PluginManager(plugin_helpers.NewPluginManagerImpl(targetDriver, &defaultConnProvider, nil, props))
	pluginServiceImpl := plugin_helpers.NewPluginServiceImpl(&pluginManager, props)
	pluginService := driver_infrastructure.PluginService(pluginServiceImpl)
	hostListProviderService := driver_infrastructure.HostListProviderService(pluginServiceImpl)

	pluginChainBuilder := ConnectionPluginChainBuilder{}
	plugins, err := pluginChainBuilder.GetPlugins(&pluginService, &pluginManager, props)
	if err != nil {
		return Container{}, err
	}
	err = pluginManager.Init(&pluginService, props, plugins)
	if err != nil {
		return Container{}, err
	}
	err = pluginManager.InitHostProvider(dsn, props, &hostListProviderService)
	if err != nil {
		return Container{}, err
	}

	return Container{
		PluginManager: &pluginManager,
		PluginService: &pluginService,
	}, nil
}
