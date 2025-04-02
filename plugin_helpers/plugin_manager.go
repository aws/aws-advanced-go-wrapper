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

package plugin_helpers

import (
	"awssql/driver_infrastructure"
	"awssql/error_util"
	"awssql/host_info_util"
	"database/sql/driver"
	"log/slog"
	"slices"
)

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

type PluginChain struct {
	execFunc     func() (any, any, bool, error)
	connectFunc  func() (any, error)
	execChain    func(pluginFunc driver_infrastructure.PluginExecFunc, execFunc func() (any, any, bool, error)) (any, any, bool, error)
	connectChain func(pluginFunc driver_infrastructure.PluginConnectFunc, execFunc func() (any, error)) (any, error)
}

func (chain *PluginChain) ExecAddToHead(plugin driver_infrastructure.ConnectionPlugin) {
	if chain.execChain == nil {
		chain.execChain = func(pluginFunc driver_infrastructure.PluginExecFunc, execFunc func() (any, any, bool, error)) (any, any, bool, error) {
			return pluginFunc(plugin, execFunc)
		}
	} else {
		pipelineSoFar := chain.execChain
		chain.execChain = func(pluginFunc driver_infrastructure.PluginExecFunc, execFunc func() (any, any, bool, error)) (any, any, bool, error) {
			return pluginFunc(plugin, func() (any, any, bool, error) { return pipelineSoFar(pluginFunc, execFunc) })
		}
	}
}

func (chain *PluginChain) ConnectAddToHead(plugin driver_infrastructure.ConnectionPlugin) {
	if chain.connectChain == nil {
		chain.connectChain = func(pluginFunc driver_infrastructure.PluginConnectFunc, connectFunc func() (any, error)) (any, error) {
			return pluginFunc(plugin, connectFunc)
		}
	} else {
		pipelineSoFar := chain.connectChain
		chain.connectChain = func(pluginFunc driver_infrastructure.PluginConnectFunc, connectFunc func() (any, error)) (any, error) {
			return pluginFunc(plugin, func() (any, error) { return pipelineSoFar(pluginFunc, connectFunc) })
		}
	}
}

func (chain *PluginChain) Execute(pluginFunc driver_infrastructure.PluginExecFunc) (any, any, bool, error) {
	if chain.execChain == nil {
		panic(error_util.GetMessage("PluginManager.pipelineNone"))
	}
	return chain.execChain(pluginFunc, chain.execFunc)
}

func (chain *PluginChain) Connect(pluginFunc driver_infrastructure.PluginConnectFunc) (any, error) {
	if chain.connectChain == nil {
		panic(error_util.GetMessage("PluginManager.pipelineNone"))
	}
	return chain.connectChain(pluginFunc, chain.connectFunc)
}

type PluginManagerImpl struct {
	targetDriver        driver.Driver
	pluginService       driver_infrastructure.PluginService
	connProviderManager driver_infrastructure.ConnectionProviderManager
	props               map[string]string
	pluginFuncMap       map[string]PluginChain
	plugins             []driver_infrastructure.ConnectionPlugin
}

func NewPluginManagerImpl(
	targetDriver driver.Driver,
	props map[string]string,
	connProviderManager driver_infrastructure.ConnectionProviderManager) *PluginManagerImpl {
	pluginFuncMap := make(map[string]PluginChain)
	return &PluginManagerImpl{
		targetDriver:        targetDriver,
		props:               props,
		pluginFuncMap:       pluginFuncMap,
		connProviderManager: connProviderManager,
	}
}

func (pluginManager *PluginManagerImpl) Init(
	pluginService driver_infrastructure.PluginService,
	plugins []driver_infrastructure.ConnectionPlugin) error {
	pluginManager.pluginService = pluginService
	pluginManager.plugins = plugins
	return nil
}

func (pluginManager *PluginManagerImpl) InitHostProvider(
	initialUrl string,
	props map[string]string,
	hostListProviderService driver_infrastructure.HostListProviderService) error {
	pluginFunc := func(plugin driver_infrastructure.ConnectionPlugin, targetFunc func() (any, any, bool, error)) (any, any, bool, error) {
		initFunc := func() error {
			_, _, _, err := targetFunc()
			return err
		}
		err := plugin.InitHostProvider(initialUrl, props, hostListProviderService, initFunc)
		if err != nil {
			return nil, nil, false, err
		}
		return nil, nil, true, nil
	}
	targetFunc := func() (any, any, bool, error) {
		return nil, nil, false, error_util.ShouldNotBeCalledError
	}
	_, _, _, err := pluginManager.ExecuteWithSubscribedPlugins(INIT_HOST_PROVIDER_METHOD, pluginFunc, targetFunc)
	if err != nil {
		return err
	} else {
		return nil
	}
}

func (pluginManager *PluginManagerImpl) Connect(
	hostInfo host_info_util.HostInfo,
	props map[string]string,
	isInitialConnection bool) (driver.Conn, error) {
	pluginFunc := func(plugin driver_infrastructure.ConnectionPlugin, targetFunc func() (any, error)) (any, error) {
		return plugin.Connect(hostInfo, props, isInitialConnection, targetFunc)
	}
	targetFunc := func() (any, error) {
		return nil, error_util.ShouldNotBeCalledError
	}
	result, err := pluginManager.ConnectWithSubscribedPlugins(CONNECT_METHOD, pluginFunc, targetFunc)
	conn, ok := result.(driver.Conn)
	if ok {
		return conn, err
	}
	return nil, err
}

func (pluginManager *PluginManagerImpl) ForceConnect(
	hostInfo host_info_util.HostInfo,
	props map[string]string,
	isInitialConnection bool) (driver.Conn, error) {
	pluginFunc := func(plugin driver_infrastructure.ConnectionPlugin, targetFunc func() (any, error)) (any, error) {
		return plugin.ForceConnect(hostInfo, props, isInitialConnection, targetFunc)
	}
	targetFunc := func() (any, error) {
		return nil, error_util.ShouldNotBeCalledError
	}
	result, err := pluginManager.ConnectWithSubscribedPlugins(FORCE_CONNECT_METHOD, pluginFunc, targetFunc)
	conn, ok := result.(driver.Conn)
	if ok {
		return conn, err
	}
	return nil, err
}

func (pluginManager *PluginManagerImpl) Execute(
	methodName string,
	executeFunc driver_infrastructure.ExecuteFunc,
	methodArgs ...any) (any, any, bool, error) {
	pluginFunc := func(plugin driver_infrastructure.ConnectionPlugin, targetFunc func() (any, any, bool, error)) (any, any, bool, error) {
		return plugin.Execute(methodName, targetFunc, methodArgs...)
	}
	return pluginManager.ExecuteWithSubscribedPlugins(methodName, pluginFunc, executeFunc)
}

func (pluginManager *PluginManagerImpl) ExecuteWithSubscribedPlugins(
	methodName string,
	pluginFunc driver_infrastructure.PluginExecFunc,
	targetFunc driver_infrastructure.ExecuteFunc) (any, any, bool, error) {
	chain, ok := pluginManager.pluginFuncMap[methodName]
	if !ok {
		chain = pluginManager.MakePluginChain(methodName, targetFunc, nil)
		pluginManager.pluginFuncMap[methodName] = chain
	}
	return chain.Execute(pluginFunc)
}

func (pluginManager *PluginManagerImpl) ConnectWithSubscribedPlugins(
	methodName string,
	pluginFunc driver_infrastructure.PluginConnectFunc,
	targetFunc driver_infrastructure.ConnectFunc) (any, error) {
	chain, ok := pluginManager.pluginFuncMap[methodName]
	if !ok {
		chain = pluginManager.MakePluginChain(methodName, nil, targetFunc)
		pluginManager.pluginFuncMap[methodName] = chain
	}
	return chain.Connect(pluginFunc)
}

func (pluginManager *PluginManagerImpl) MakePluginChain(
	name string,
	execFunc func() (any, any, bool, error),
	connectFunc func() (any, error)) PluginChain {
	chain := PluginChain{execFunc: execFunc, connectFunc: connectFunc}
	for i := len(pluginManager.plugins) - 1; i >= 0; i-- {
		currentPlugin := pluginManager.plugins[i]
		pluginSubscribedMethods := currentPlugin.GetSubscribedMethods()
		if slices.Contains(pluginSubscribedMethods, ALL_METHODS) || slices.Contains(pluginSubscribedMethods, name) {
			if execFunc != nil {
				chain.ExecAddToHead(currentPlugin)
			} else if connectFunc != nil {
				chain.ConnectAddToHead(currentPlugin)
			}
		}
	}

	return chain
}

func (pluginManager *PluginManagerImpl) AcceptsStrategy(role host_info_util.HostRole, strategy string) bool {
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

func (pluginManager *PluginManagerImpl) NotifyHostListChanged(changes map[string]map[driver_infrastructure.HostChangeOptions]bool) {
	notifyFunc := func(plugin driver_infrastructure.ConnectionPlugin, targetFunc func() (any, any, bool, error)) (any, any, bool, error) {
		plugin.NotifyHostListChanged(changes)
		return nil, nil, true, nil
	}
	_ = pluginManager.NotifySubscribedPlugins(NOTIFY_HOST_LIST_CHANGED_METHOD, notifyFunc, nil)
}

func (pluginManager *PluginManagerImpl) NotifyConnectionChanged(
	changes map[driver_infrastructure.HostChangeOptions]bool,
	skipNotificationForThisPlugin driver_infrastructure.ConnectionPlugin) map[driver_infrastructure.OldConnectionSuggestedAction]bool {
	result := make(map[driver_infrastructure.OldConnectionSuggestedAction]bool)
	var pluginFunc driver_infrastructure.PluginExecFunc = func(plugin driver_infrastructure.ConnectionPlugin, foo func() (any, any, bool, error)) (any, any, bool, error) {
		pluginOptions := plugin.NotifyConnectionChanged(changes)
		result[pluginOptions] = true
		return nil, nil, true, nil
	}
	_ = pluginManager.NotifySubscribedPlugins(NOTIFY_CONNECTION_CHANGED_METHOD, pluginFunc, skipNotificationForThisPlugin)
	return result
}

func (pluginManager *PluginManagerImpl) NotifySubscribedPlugins(
	methodName string,
	pluginFunc driver_infrastructure.PluginExecFunc,
	skipNotificationForThisPlugin driver_infrastructure.ConnectionPlugin) error {
	for i := 0; i < len(pluginManager.plugins); i++ {
		currentPlugin := pluginManager.plugins[i]
		if currentPlugin == skipNotificationForThisPlugin {
			continue
		}

		pluginSubscribedMethods := currentPlugin.GetSubscribedMethods()
		isSubscribed := slices.Contains(pluginSubscribedMethods, ALL_METHODS) || slices.Contains(pluginSubscribedMethods, methodName)

		if isSubscribed {
			_, _, _, err := pluginFunc(currentPlugin, func() (any, any, bool, error) { return nil, nil, true, nil })
			return err
		}
	}
	return nil
}

func (pluginManager *PluginManagerImpl) GetHostInfoByStrategy(
	role host_info_util.HostRole,
	strategy string,
	hosts []*host_info_util.HostInfo) (*host_info_util.HostInfo, error) {
	for i := 0; i < len(pluginManager.plugins); i++ {
		currentPlugin := pluginManager.plugins[i]
		isSubscribed := slices.Contains(currentPlugin.GetSubscribedMethods(), strategy)

		if isSubscribed {
			host, err := currentPlugin.GetHostInfoByStrategy(role, strategy, hosts)

			if err == nil {
				return host, nil
			}
		}
	}

	return nil, error_util.NewUnsupportedStrategyError(
		error_util.GetMessage("The wrapper does not support the requested host selection strategy: " + strategy))
}

func (pluginManager *PluginManagerImpl) GetDefaultConnectionProvider() driver_infrastructure.ConnectionProvider {
	return pluginManager.connProviderManager.DefaultProvider
}

func (pluginManager *PluginManagerImpl) GetEffectiveConnectionProvider() driver_infrastructure.ConnectionProvider {
	return pluginManager.connProviderManager.EffectiveProvider
}

func (pluginManager *PluginManagerImpl) GetConnectionProviderManager() driver_infrastructure.ConnectionProviderManager {
	return pluginManager.connProviderManager
}

func (pluginManager *PluginManagerImpl) ReleaseResources() {
	slog.Info(error_util.GetMessage("PluginManagerImpl.releaseResources"))

	// This step allows all plugins a chance to perform any last tasks before shutting down.
	for i := 0; i < len(pluginManager.plugins); i++ {
		currentPlugin := pluginManager.plugins[i]
		currentPluginCanReleaseResources, ok := currentPlugin.(driver_infrastructure.CanReleaseResources)

		if ok {
			currentPluginCanReleaseResources.ReleaseResources()
		}
	}

	canReleaseResources, ok := pluginManager.pluginService.(driver_infrastructure.CanReleaseResources)
	if ok {
		canReleaseResources.ReleaseResources()
	}
}
