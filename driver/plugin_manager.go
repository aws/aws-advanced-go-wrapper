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
	"slices"
)

type ConnectFunc func() (driver.Conn, error)
type ExecuteFunc func() (any, any, bool, error)

const (
	ALL_METHODS                      = "*"
	CONNECT_METHOD                   = "connect"
	FORCE_CONNECT_METHOD             = "forceConnect"
	ACCEPTS_STRATEGY_METHOD          = "acceptsStrategy"
	GET_HOST_INFO_BY_STRATEGY_METHOD = "getHostInfoByStrategy"
	INIT_HOST_PROVIDER_METHOD        = "initHostProvider"
	NOTIFY_CONNECTION_CHANGED_METHOD = "notifyConnectionChanged"
	NOTIFY_HOST_LIST_CHANGED_METHOD  = "notifyHostListChanged"
)

type PluginFunc func(plugin ConnectionPlugin, targetFunc func() (any, error)) (any, error)

type PluginChain struct {
	targetFunc func() (any, error)
	chain      func(pluginFunc PluginFunc, targetFunc func() (any, error)) (any, error)
}

func (chain *PluginChain) AddToHead(plugin ConnectionPlugin) {
	if chain.chain == nil {
		chain.chain = func(pluginFunc PluginFunc, targetFunc func() (any, error)) (any, error) {
			return pluginFunc(plugin, targetFunc)
		}
	} else {
		pipelineSoFar := chain.chain
		chain.chain = func(pluginFunc PluginFunc, targetFunc func() (any, error)) (any, error) {
			return pluginFunc(plugin, func() (any, error) { return pipelineSoFar(pluginFunc, targetFunc) })
		}
	}
}

func (chain *PluginChain) Execute(pluginFunc PluginFunc) (any, error) {
	if chain.chain == nil {
		panic(GetMessage("PluginManager.pipelineNone"))
	}
	return chain.chain(pluginFunc, chain.targetFunc)
}

type PluginManager struct {
	targetDriver          driver.Driver
	pluginService         *PluginService
	connProviderManager   ConnectionProviderManager
	defaultConnProvider   ConnectionProvider
	effectiveConnProvider ConnectionProvider
	props                 map[string]any
	pluginFuncMap         map[string]PluginChain
	plugins               []ConnectionPlugin
}

func NewPluginManager(
	targetDriver driver.Driver,
	defaultConnProvider ConnectionProvider,
	effectiveConnProvider ConnectionProvider,
	props map[string]any) *PluginManager {
	pluginFuncMap := make(map[string]PluginChain)
	return &PluginManager{
		targetDriver:          targetDriver,
		defaultConnProvider:   defaultConnProvider,
		effectiveConnProvider: effectiveConnProvider,
		props:                 props,
		pluginFuncMap:         pluginFuncMap,
	}
}

func (pluginManager *PluginManager) Init(pluginService PluginService, props map[string]any, plugins []ConnectionPlugin) error {
	pluginManager.pluginService = &pluginService
	if len(plugins) == 0 {
		pluginChainBuilder := ConnectionPluginChainBuilder{}
		chainPlugins, err := pluginChainBuilder.GetPlugins(&pluginService, *pluginManager, props)
		if err != nil {
			return err
		}
		pluginManager.plugins = chainPlugins
	} else {
		pluginManager.plugins = plugins
	}

	return nil
}

func (pluginManager *PluginManager) InitHostProvider(
	initialUrl string,
	props map[string]any,
	hostListProviderService HostListProviderService) {
	pluginFunc := func(plugin ConnectionPlugin, targetFunc func() (any, error)) (any, error) {
		initFunc := func() error {
			_, err := targetFunc()
			return err
		}
		err := plugin.InitHostProvider(initialUrl, props, hostListProviderService, initFunc)
		if err != nil {
			return nil, err
		}
		return nil, nil
	}
	targetFunc := func() (any, error) {
		return nil, SHOULD_NOT_BE_CALLED_ERROR
	}
	_, err := pluginManager.ExecuteWithSubscribedPlugins(INIT_HOST_PROVIDER_METHOD, pluginFunc, targetFunc)
	if err != nil {
		return
	}
}

func (pluginManager *PluginManager) Connect(
	hostInfo HostInfo,
	props map[string]any,
	isInitialConnection bool) (driver.Conn, error) {
	pluginFunc := func(plugin ConnectionPlugin, targetFunc func() (any, error)) (any, error) {
		return plugin.Connect(hostInfo, props, isInitialConnection, targetFunc)
	}
	targetFunc := func() (any, error) {
		return nil, SHOULD_NOT_BE_CALLED_ERROR
	}
	result, err := pluginManager.ExecuteWithSubscribedPlugins(CONNECT_METHOD, pluginFunc, targetFunc)
	conn, ok := result.(driver.Conn)
	if ok {
		return conn, err
	}
	return nil, err
}

func (pluginManager *PluginManager) ForceConnect(
	hostInfo HostInfo,
	props map[string]any,
	isInitialConnection bool) (driver.Conn, error) {
	pluginFunc := func(plugin ConnectionPlugin, targetFunc func() (any, error)) (any, error) {
		return plugin.ForceConnect(hostInfo, props, isInitialConnection, targetFunc)
	}
	targetFunc := func() (any, error) {
		return nil, SHOULD_NOT_BE_CALLED_ERROR
	}
	result, err := pluginManager.ExecuteWithSubscribedPlugins(FORCE_CONNECT_METHOD, pluginFunc, targetFunc)
	conn, ok := result.(driver.Conn)
	if ok {
		return conn, err
	}
	return nil, err
}

func (pluginManager *PluginManager) Execute(
	methodName string,
	executeFunc ExecuteFunc,
	methodArgs ...any) (any, error) {
	pluginFunc := func(plugin ConnectionPlugin, targetFunc func() (any, error)) (any, error) {
		return plugin.Execute(methodName, targetFunc, methodArgs...)
	}
	return pluginManager.ExecuteWithSubscribedPlugins(methodName, pluginFunc, executeFunc)
}

func (pluginManager *PluginManager) ExecuteWithSubscribedPlugins(
	methodName string,
	pluginFunc PluginFunc,
	targetFunc ExecuteFunc) (any, error) {
	chain, ok := pluginManager.pluginFuncMap[methodName]
	if !ok {
		chain = pluginManager.MakePluginChain(methodName, targetFunc)
		pluginManager.pluginFuncMap[methodName] = chain
	}
	return chain.Execute(pluginFunc)
}

func (pluginManager *PluginManager) MakePluginChain(name string, targetFunc func() (any, error)) PluginChain {
	chain := PluginChain{targetFunc: targetFunc}
	for i := len(pluginManager.plugins) - 1; i >= 0; i-- {
		currentPlugin := pluginManager.plugins[i]
		pluginSubscribedMethods := currentPlugin.GetSubscribedMethods()
		if slices.Contains(pluginSubscribedMethods, ALL_METHODS) || slices.Contains(pluginSubscribedMethods, name) {
			chain.AddToHead(currentPlugin)
		}
	}

	return chain
}

func (pluginManager *PluginManager) AcceptsStrategy(role HostRole, strategy string) bool {
	for i := 0; i < len(pluginManager.plugins); i++ {
		currentPlugin := pluginManager.plugins[i]
		pluginSubscribedMethods := currentPlugin.GetSubscribedMethods()
		isSubscribed := slices.Contains(pluginSubscribedMethods, strategy) || slices.Contains(pluginSubscribedMethods, ALL_METHODS)

		if isSubscribed && currentPlugin.AcceptsStrategy(role, strategy) {
			return true
		}
	}

	return false
}

func (pluginManager *PluginManager) NotifyHostListChanged(changes map[string]map[HostChangeOptions]bool) {
	notifyFunc := func(plugin ConnectionPlugin, targetFunc func() (any, error)) (any, error) {
		plugin.NotifyHostListChanged(changes)
		return nil, nil
	}
	_ = pluginManager.NotifySubscribedPlugins(NOTIFY_HOST_LIST_CHANGED_METHOD, notifyFunc, nil)
}

func (pluginManager *PluginManager) NotifyConnectionChanged(
	changes map[HostChangeOptions]bool,
	skipNotificationForThisPlugin ConnectionPlugin) map[OldConnectionSuggestedAction]bool {
	result := make(map[OldConnectionSuggestedAction]bool)
	var pluginFunc PluginFunc = func(plugin ConnectionPlugin, foo func() (any, error)) (any, error) {
		pluginOptions := plugin.NotifyConnectionChanged(changes)
		result[pluginOptions] = true
		return nil, nil
	}
	_ = pluginManager.NotifySubscribedPlugins(NOTIFY_CONNECTION_CHANGED_METHOD, pluginFunc, skipNotificationForThisPlugin)
	return result
}

func (pluginManager *PluginManager) NotifySubscribedPlugins(
	methodName string,
	pluginFunc PluginFunc,
	skipNotificationForThisPlugin ConnectionPlugin) error {
	for i := 0; i < len(pluginManager.plugins); i++ {
		if pluginManager.plugins[i] == skipNotificationForThisPlugin {
			continue
		}

		pluginSubscribedMethods := pluginManager.plugins[i].GetSubscribedMethods()
		isSubscribed := slices.Contains(pluginSubscribedMethods, ALL_METHODS) || slices.Contains(pluginSubscribedMethods, methodName)

		if isSubscribed {
			_, err := pluginFunc(pluginManager.plugins[i], func() (any, error) { return nil, nil })
			return err
		}
	}
	return nil
}

func (pluginManager *PluginManager) GetHostInfoByStrategy(role HostRole, strategy string, hosts []HostInfo) (HostInfo, error) {
	for i := 0; i < len(pluginManager.plugins); i++ {
		currentPlugin := pluginManager.plugins[i]
		pluginSubscribedMethods := currentPlugin.GetSubscribedMethods()
		isSubscribed := slices.Contains(pluginSubscribedMethods, strategy)

		var host HostInfo
		var err error
		if isSubscribed {
			host, err = currentPlugin.GetHostInfoByStrategy(role, strategy, hosts)

			if err != nil {
				return host, nil
			}
		}
	}

	return HostInfo{}, errors.New("The driver does not support the requested host selection strategy: " + strategy)
}
