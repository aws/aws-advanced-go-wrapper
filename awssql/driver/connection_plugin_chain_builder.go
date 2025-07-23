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
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
)

const WEIGHT_RELATIVE_TO_PRIOR_PLUGIN = -1

type PluginFactoryWeight struct {
	pluginFactory driver_infrastructure.ConnectionPluginFactory
	weight        int
}

var pluginWeightByCode = map[string]int{
	"bg":                550,
	"readWriteSplitting": 600,
	"failover":          700,
	"efm":               800,
	"limitless":         950,
	"iam":               1000,
	"awsSecretsManager": 1100,
	"federatedAuth":     1200,
	"okta":              1300,
	"executionTime":     WEIGHT_RELATIVE_TO_PRIOR_PLUGIN,
}

type ConnectionPluginChainBuilder struct {
}

func (builder *ConnectionPluginChainBuilder) GetPlugins(
	pluginService driver_infrastructure.PluginService,
	pluginManager driver_infrastructure.PluginManager,
	props map[string]string,
	availablePlugins map[string]driver_infrastructure.ConnectionPluginFactory) ([]driver_infrastructure.ConnectionPlugin, error) {
	var resultPlugins []driver_infrastructure.ConnectionPlugin
	var pluginFactoryWeights []PluginFactoryWeight
	usingDefault := false

	pluginCodes := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.PLUGINS)
	if pluginCodes == property_util.DEFAULT_PLUGINS {
		usingDefault = true
	}

	pluginCodes = strings.ReplaceAll(strings.TrimSpace(pluginCodes), " ", "")
	var pluginCodesSlice []string
	if len(pluginCodes) != 0 && strings.ToLower(pluginCodes) != "none" {
		pluginCodesSlice = strings.Split(pluginCodes, ",")
	}
	lastWeight := 0
	for _, pluginCode := range pluginCodesSlice {
		if pluginCode == "" {
			continue
		}
		factory, ok := availablePlugins[pluginCode]
		if !ok {
			return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("ConnectionPluginManager.unknownPluginCode", pluginCode))
		}
		if lastWeight == WEIGHT_RELATIVE_TO_PRIOR_PLUGIN {
			lastWeight++
		} else {
			lastWeight = pluginWeightByCode[pluginCode]
		}
		pluginFactoryWeights = append(pluginFactoryWeights, PluginFactoryWeight{factory, lastWeight})
	}

	autoSort := property_util.GetVerifiedWrapperPropertyValue[bool](props, property_util.AUTO_SORT_PLUGIN_ORDER)
	pluginsSorted := false
	if !usingDefault && len(pluginFactoryWeights) > 1 && autoSort {
		sort.Slice(pluginFactoryWeights, func(i, j int) bool {
			return pluginFactoryWeights[i].weight < pluginFactoryWeights[j].weight
		})
		pluginsSorted = true
	}

	for _, pluginFactoryFuncWeight := range pluginFactoryWeights {
		plugin, err := pluginFactoryFuncWeight.pluginFactory.GetInstance(pluginService, props)
		if err != nil {
			return nil, err
		}
		if plugin != nil {
			resultPlugins = append(resultPlugins, plugin)
		}
	}

	defaultPlugin := driver_infrastructure.ConnectionPlugin(&plugins.DefaultPlugin{
		PluginService:       pluginService,
		DefaultConnProvider: pluginManager.GetDefaultConnectionProvider(),
		ConnProviderManager: pluginManager.GetConnectionProviderManager(),
	})
	resultPlugins = append(resultPlugins, defaultPlugin)
	if pluginsSorted {
		slog.Info(fmt.Sprintf("Plugins order has been rearranged. The following order is in effect: '%v'.", getFactoryOrder(resultPlugins)))
	}

	return resultPlugins, nil
}

func getFactoryOrder(pluginFactoryFuncWeights []driver_infrastructure.ConnectionPlugin) string {
	var pluginFactories []string
	for _, pluginFactoryFuncWeight := range pluginFactoryFuncWeights {
		pluginFactories = append(pluginFactories, fmt.Sprintf("%T", pluginFactoryFuncWeight))
	}
	return strings.Join(pluginFactories, ",")
}
