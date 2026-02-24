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

package limitless

import (
	"database/sql/driver"
	"errors"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

type LimitlessPluginFactory struct {
}

func (factory LimitlessPluginFactory) GetInstance(servicesContainer driver_infrastructure.ServicesContainer,
	props *utils.RWMap[string, string]) (driver_infrastructure.ConnectionPlugin, error) {
	return NewLimitlessPlugin(servicesContainer, props)
}

func (factory LimitlessPluginFactory) ClearCaches() {}

func NewLimitlessPluginFactory() driver_infrastructure.ConnectionPluginFactory {
	return LimitlessPluginFactory{}
}

type LimitlessPlugin struct {
	plugins.BaseConnectionPlugin
	pluginService driver_infrastructure.PluginService
	props         *utils.RWMap[string, string]
	routerService LimitlessRouterService
}

func NewLimitlessPlugin(servicesContainer driver_infrastructure.ServicesContainer, props *utils.RWMap[string, string]) (*LimitlessPlugin, error) {
	validateShardGroupUrl := property_util.GetVerifiedWrapperPropertyValue[bool](props, property_util.LIMITLESS_USE_SHARD_GROUP_URL)
	if validateShardGroupUrl {
		host := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.HOST)
		isShardGroupUrl := utils.IsLimitlessDbShardGroupDns(host)
		if !isShardGroupUrl {
			return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("LimitlessPlugin.expectedShardGroupUrl", host))
		}
	}

	return &LimitlessPlugin{
		pluginService: servicesContainer.GetPluginService(),
		props:         props,
	}, nil
}

// Note: This method is for testing purposes.
func NewLimitlessPluginWithRouterService(pluginService driver_infrastructure.PluginService, props *utils.RWMap[string, string],
	routerService LimitlessRouterService) *LimitlessPlugin {
	return &LimitlessPlugin{
		pluginService: pluginService,
		props:         props,
		routerService: routerService,
	}
}

func (plugin *LimitlessPlugin) GetPluginCode() string {
	return driver_infrastructure.LIMITLESS_PLUGIN_CODE
}

func (plugin *LimitlessPlugin) GetSubscribedMethods() []string {
	return []string{plugin_helpers.CONNECT_METHOD}
}

func (plugin *LimitlessPlugin) Connect(
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	var conn driver.Conn = nil

	// Dialect check
	dialect := plugin.pluginService.GetDialect()
	if !IsDialectLimitless(dialect) {
		var err error
		conn, err = connectFunc(props)
		if err != nil {
			return nil, err
		}
		refreshDialect := plugin.pluginService.GetDialect()
		if !IsDialectLimitless(refreshDialect) {
			return nil, errors.New(error_util.GetMessage("LimitlessPlugin.unsupportedDialectOrDatabase", refreshDialect))
		}
	}

	// Init LimitlessRouterService
	plugin.initLimitlessRouterService()
	if isInitialConnection {
		err := plugin.routerService.StartMonitoring(
			hostInfo,
			props,
			property_util.GetRefreshRateValue(props, property_util.LIMITLESS_MONITORING_INTERVAL_MS))
		if err != nil {
			return nil, err
		}
	}

	// Establish Connection
	context := NewConnectionContext(*hostInfo, props, conn, connectFunc, nil, plugin)
	err := plugin.routerService.EstablishConnection(context)
	if err != nil {
		return nil, err
	}

	if context.connection != nil {
		return context.connection, nil
	}
	return nil, errors.New(error_util.GetMessage("LimitlessPlugin.failedToConnectToHost", hostInfo.Host))
}

func (plugin *LimitlessPlugin) initLimitlessRouterService() {
	if plugin.routerService == nil {
		plugin.routerService = NewLimitlessRouterServiceImpl(plugin.pluginService, plugin.props)
	}
}
