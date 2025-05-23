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
	"awssql/utils/telemetry"
)

type Container struct {
	PluginManager           driver_infrastructure.PluginManager
	PluginService           driver_infrastructure.PluginService
	HostListProviderService driver_infrastructure.HostListProviderService
	Props                   map[string]string
}

func NewContainer(dsn string) (Container, error) {
	props, parseErr := utils.ParseDsn(dsn)
	if parseErr != nil {
		return Container{}, parseErr
	}
	targetDriver, getTargetDriverErr := GetTargetDriver(dsn, props)
	if getTargetDriverErr != nil {
		return Container{}, parseErr
	}

	targetDriverDialectManager := driver_infrastructure.DriverDialectManager{}
	targetDriverDialect, err := targetDriverDialectManager.GetDialect(targetDriver, props)
	if err != nil {
		return Container{}, err
	}

	defaultConnProvider := driver_infrastructure.NewDriverConnectionProvider(targetDriver)
	connectionProviderManager := driver_infrastructure.ConnectionProviderManager{DefaultProvider: defaultConnProvider}

	telemetryFactory, err := telemetry.NewDefaultTelemetryFactory(props)
	if err != nil {
		return Container{}, err
	}
	pluginManager := driver_infrastructure.PluginManager(plugin_helpers.NewPluginManagerImpl(targetDriver, props, connectionProviderManager, telemetryFactory))
	pluginServiceImpl, err := plugin_helpers.NewPluginServiceImpl(pluginManager, targetDriverDialect, props, dsn)
	if err != nil {
		return Container{}, err
	}
	pluginService := driver_infrastructure.PluginService(pluginServiceImpl)

	pluginChainBuilder := ConnectionPluginChainBuilder{}
	plugins, err := pluginChainBuilder.GetPlugins(pluginService, pluginManager, props)
	if err != nil {
		return Container{}, err
	}

	err = pluginManager.Init(pluginService, plugins)
	if err != nil {
		return Container{}, err
	}

	hostListProviderService := driver_infrastructure.HostListProviderService(pluginServiceImpl)
	provider := hostListProviderService.CreateHostListProvider(props, dsn)
	hostListProviderService.SetHostListProvider(provider)

	return Container{
		PluginManager:           pluginManager,
		PluginService:           pluginService,
		HostListProviderService: hostListProviderService,
		Props:                   props,
	}, nil
}
