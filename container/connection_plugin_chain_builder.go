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
	"awssql/plugins"
	"fmt"
	"log/slog"
	"reflect"
	"sort"
	"strings"
)

const WEIGHT_RELATIVE_TO_PRIOR_PLUGIN = -1

type PluginFactoryFunc func() driver_infrastructure.ConnectionPluginFactory

type PluginFactoryFuncWeight struct {
	pluginFactoryFunc PluginFactoryFunc
	weight            int
}

var pluginFactoryFuncByCode = map[string]PluginFactoryFunc{}
var pluginWeightByCode = map[string]int{}

type ConnectionPluginChainBuilder struct {
}

func (builder *ConnectionPluginChainBuilder) GetPlugins(
	pluginService *driver_infrastructure.PluginService,
	pluginManager *driver_infrastructure.PluginManager,
	props map[string]any) ([]*driver_infrastructure.ConnectionPlugin, error) {
	var resultPlugins []*driver_infrastructure.ConnectionPlugin
	var pluginFactoryFuncWeights []PluginFactoryFuncWeight
	usingDefault := false

	pluginCodes := driver_infrastructure.GetVerifiedWrapperPropertyValue[string](props, driver_infrastructure.PLUGINS)
	if pluginCodes == driver_infrastructure.DEFAULT_PLUGINS {
		usingDefault = true
	}

	pluginCodes = strings.ReplaceAll(strings.TrimSpace(pluginCodes), " ", "")
	pluginCodesSlice := strings.Split(pluginCodes, ",")
	lastWeight := 0
	for _, pluginCode := range pluginCodesSlice {
		factoryFunc, ok := pluginFactoryFuncByCode[pluginCode]
		if !ok {
			return nil, &driver_infrastructure.AwsWrapperError{
				Message:   driver_infrastructure.GetMessage("ConnectionPluginManager.unknownPluginCode", pluginCode),
				ErrorType: driver_infrastructure.GenericAwsWrapperErrorType,
			}
		}
		if lastWeight == WEIGHT_RELATIVE_TO_PRIOR_PLUGIN {
			lastWeight++
		} else {
			lastWeight = pluginWeightByCode[pluginCode]
		}
		pluginFactoryFuncWeights = append(pluginFactoryFuncWeights, PluginFactoryFuncWeight{factoryFunc, lastWeight})
	}

	autoSort := driver_infrastructure.GetVerifiedWrapperPropertyValue[bool](props, driver_infrastructure.AUTO_SORT_PLUGIN_ORDER)
	pluginsSorted := false
	if !usingDefault && len(pluginFactoryFuncWeights) > 1 && autoSort {
		sort.Slice(pluginFactoryFuncWeights, func(i, j int) bool {
			return pluginFactoryFuncWeights[i].weight < pluginFactoryFuncWeights[j].weight
		})
		pluginsSorted = true
	}

	for _, pluginFactoryFuncWeight := range pluginFactoryFuncWeights {
		pluginFactory := pluginFactoryFuncWeight.pluginFactoryFunc()
		plugin, err := pluginFactory.GetInstance(pluginService, props)
		if err != nil {
			return nil, err
		}
		resultPlugins = append(resultPlugins, plugin)
	}

	defaultPlugin := driver_infrastructure.ConnectionPlugin(&plugins.DefaultPlugin{
		PluginService:       pluginService,
		DefaultConnProvider: (*pluginManager).GetDefaultConnectionProvider(),
		ConnProviderManager: (*pluginManager).GetConnectionProviderManager(),
	})
	resultPlugins = append(resultPlugins, &defaultPlugin)
	if pluginsSorted {
		slog.Info(fmt.Sprintf("Plugins order has been rearranged. The following order is in effect: '%v'.", getFactoryOrder(resultPlugins)))
	}

	return resultPlugins, nil
}

func getFactoryOrder(pluginFactoryFuncWeights []*driver_infrastructure.ConnectionPlugin) string {
	var pluginFactories []string
	for i := 0; i < len(pluginFactoryFuncWeights); i++ {
		pluginFactories = append(pluginFactories, reflect.TypeOf(*pluginFactoryFuncWeights[i]).String())
	}
	return strings.Join(pluginFactories, ",")
}
