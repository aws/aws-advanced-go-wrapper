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
	"awssql/utils"
	"awssql/utils/telemetry"
	"context"
	"database/sql/driver"
	"log/slog"
	"slices"
	"sync"
)

const (
	ALL_METHODS                      = "*"
	CONNECT_METHOD                   = "Conn.Connect"
	FORCE_CONNECT_METHOD             = "Conn.ForceConnect"
	ACCEPTS_STRATEGY_METHOD          = "acceptsStrategy"
	GET_HOST_INFO_BY_STRATEGY_METHOD = "getHostInfoByStrategy"
	GET_HOST_SELECT_STRATEGY_METHOD  = "getHostSelectorStrategy"
	INIT_HOST_PROVIDER_METHOD        = "initHostProvider"
	NOTIFY_CONNECTION_CHANGED_METHOD = "notifyConnectionChanged"
	NOTIFY_HOST_LIST_CHANGED_METHOD  = "notifyHostListChanged"
)

type PluginChain struct {
	execChain    func(pluginFunc driver_infrastructure.PluginExecFunc, execFunc func() (any, any, bool, error)) (any, any, bool, error)
	connectChain func(pluginFunc driver_infrastructure.PluginConnectFunc, connectFunc func() (driver.Conn, error)) (driver.Conn, error)
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
		chain.connectChain = func(pluginFunc driver_infrastructure.PluginConnectFunc, connectFunc func() (driver.Conn, error)) (driver.Conn, error) {
			return pluginFunc(plugin, connectFunc)
		}
	} else {
		pipelineSoFar := chain.connectChain
		chain.connectChain = func(pluginFunc driver_infrastructure.PluginConnectFunc, connectFunc func() (driver.Conn, error)) (driver.Conn, error) {
			return pluginFunc(plugin, func() (driver.Conn, error) { return pipelineSoFar(pluginFunc, connectFunc) })
		}
	}
}

func (chain *PluginChain) Execute(pluginFunc driver_infrastructure.PluginExecFunc, execFunc func() (any, any, bool, error)) (any, any, bool, error) {
	if chain.execChain == nil {
		slog.Warn(error_util.GetMessage("PluginManager.pipelineNone"))
		return nil, nil, false, error_util.NewGenericAwsWrapperError(error_util.GetMessage("PluginManager.pipelineNone"))
	}
	return chain.execChain(pluginFunc, execFunc)
}

func (chain *PluginChain) Connect(pluginFunc driver_infrastructure.PluginConnectFunc, connectFunc func() (driver.Conn, error)) (driver.Conn, error) {
	if chain.connectChain == nil {
		slog.Warn(error_util.GetMessage("PluginManager.pipelineNone"))
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("PluginManager.pipelineNone"))
	}
	return chain.connectChain(pluginFunc, connectFunc)
}

type PluginManagerImpl struct {
	targetDriver        driver.Driver
	pluginService       driver_infrastructure.PluginService
	connProviderManager driver_infrastructure.ConnectionProviderManager
	props               map[string]string
	pluginFuncMap       map[string]PluginChain
	plugins             []driver_infrastructure.ConnectionPlugin
	telemetryFactory    telemetry.TelemetryFactory
	telemetryCtx        context.Context
	telemetryCtxLock    sync.Mutex
}

func NewPluginManagerImpl(
	targetDriver driver.Driver,
	props map[string]string,
	connProviderManager driver_infrastructure.ConnectionProviderManager,
	telemetryFactory telemetry.TelemetryFactory) *PluginManagerImpl {
	pluginFuncMap := make(map[string]PluginChain)
	return &PluginManagerImpl{
		targetDriver:        targetDriver,
		props:               props,
		connProviderManager: connProviderManager,
		pluginFuncMap:       pluginFuncMap,
		telemetryFactory:    telemetryFactory,
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
	parentCtx := pluginManager.GetTelemetryContext()
	telemetryCtx, ctx := pluginManager.telemetryFactory.OpenTelemetryContext(telemetry.TELEMETRY_INIT_HOST_PROVIDER, telemetry.NESTED, parentCtx)
	pluginManager.SetTelemetryContext(ctx)
	defer func() {
		telemetryCtx.CloseContext()
		pluginManager.SetTelemetryContext(parentCtx)
	}()

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
	_, _, _, err := pluginManager.executeWithSubscribedPlugins(INIT_HOST_PROVIDER_METHOD, pluginFunc, targetFunc)
	if err != nil {
		return err
	} else {
		return nil
	}
}

func (pluginManager *PluginManagerImpl) Connect(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	isInitialConnection bool) (driver.Conn, error) {
	parentCtx := pluginManager.GetTelemetryContext()
	telemetryCtx, ctx := pluginManager.telemetryFactory.OpenTelemetryContext(telemetry.TELEMETRY_CONNECT, telemetry.NESTED, parentCtx)
	pluginManager.SetTelemetryContext(ctx)
	defer func() {
		telemetryCtx.CloseContext()
		pluginManager.SetTelemetryContext(parentCtx)
	}()

	pluginFunc := func(plugin driver_infrastructure.ConnectionPlugin, targetFunc func() (driver.Conn, error)) (driver.Conn, error) {
		parentCtx1 := pluginManager.GetTelemetryContext()
		telemetryCtx1, ctx1 := pluginManager.telemetryFactory.OpenTelemetryContext(utils.GetStructName(plugin), telemetry.NESTED, parentCtx1)
		pluginManager.SetTelemetryContext(ctx1)
		defer func() {
			telemetryCtx1.CloseContext()
			pluginManager.SetTelemetryContext(parentCtx1)
		}()
		return plugin.Connect(hostInfo, props, isInitialConnection, targetFunc)
	}
	targetFunc := func() (driver.Conn, error) {
		return nil, error_util.ShouldNotBeCalledError
	}
	return pluginManager.connectWithSubscribedPlugins(CONNECT_METHOD, pluginFunc, targetFunc)
}

func (pluginManager *PluginManagerImpl) ForceConnect(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	isInitialConnection bool) (driver.Conn, error) {
	pluginFunc := func(plugin driver_infrastructure.ConnectionPlugin, targetFunc func() (driver.Conn, error)) (driver.Conn, error) {
		return plugin.ForceConnect(hostInfo, props, isInitialConnection, targetFunc)
	}
	targetFunc := func() (driver.Conn, error) {
		return nil, error_util.ShouldNotBeCalledError
	}
	return pluginManager.connectWithSubscribedPlugins(FORCE_CONNECT_METHOD, pluginFunc, targetFunc)
}

func (pluginManager *PluginManagerImpl) Execute(
	methodName string,
	executeFunc driver_infrastructure.ExecuteFunc,
	methodArgs ...any) (any, any, bool, error) {
	pluginFunc := func(plugin driver_infrastructure.ConnectionPlugin, targetFunc func() (any, any, bool, error)) (any, any, bool, error) {
		parentCtx := pluginManager.GetTelemetryContext()
		telemetryCtx, ctx := pluginManager.telemetryFactory.OpenTelemetryContext(utils.GetStructName(plugin), telemetry.NESTED, parentCtx)
		pluginManager.SetTelemetryContext(ctx)
		defer func() {
			telemetryCtx.CloseContext()
			pluginManager.SetTelemetryContext(parentCtx)
		}()
		return plugin.Execute(methodName, targetFunc, methodArgs...)
	}
	executeFuncWithTelemetry := func() (any, any, bool, error) {
		parentCtx := pluginManager.GetTelemetryContext()
		telemetryCtx, ctx := pluginManager.telemetryFactory.OpenTelemetryContext(methodName, telemetry.NESTED, parentCtx)
		pluginManager.SetTelemetryContext(ctx)
		defer func() {
			telemetryCtx.CloseContext()
			pluginManager.SetTelemetryContext(parentCtx)
		}()
		return executeFunc()
	}
	return pluginManager.executeWithSubscribedPlugins(methodName, pluginFunc, executeFuncWithTelemetry)
}

func (pluginManager *PluginManagerImpl) executeWithSubscribedPlugins(
	methodName string,
	pluginFunc driver_infrastructure.PluginExecFunc,
	targetFunc driver_infrastructure.ExecuteFunc) (any, any, bool, error) {
	chain, ok := pluginManager.pluginFuncMap[methodName]
	if !ok {
		chain = pluginManager.makePluginChain(methodName, true)
		pluginManager.pluginFuncMap[methodName] = chain
	}
	return chain.Execute(pluginFunc, targetFunc)
}

func (pluginManager *PluginManagerImpl) connectWithSubscribedPlugins(
	methodName string,
	pluginFunc driver_infrastructure.PluginConnectFunc,
	targetFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	chain, ok := pluginManager.pluginFuncMap[methodName]
	if !ok {
		chain = pluginManager.makePluginChain(methodName, false)
		pluginManager.pluginFuncMap[methodName] = chain
	}
	return chain.Connect(pluginFunc, targetFunc)
}

func (pluginManager *PluginManagerImpl) makePluginChain(
	name string,
	creatingExecChain bool) PluginChain {
	chain := PluginChain{}
	for i := len(pluginManager.plugins) - 1; i >= 0; i-- {
		currentPlugin := pluginManager.plugins[i]
		pluginSubscribedMethods := currentPlugin.GetSubscribedMethods()
		if slices.Contains(pluginSubscribedMethods, ALL_METHODS) || slices.Contains(pluginSubscribedMethods, name) {
			if creatingExecChain {
				chain.ExecAddToHead(currentPlugin)
			} else {
				chain.ConnectAddToHead(currentPlugin)
			}
		}
	}

	return chain
}

func (pluginManager *PluginManagerImpl) AcceptsStrategy(strategy string) bool {
	for i := 0; i < len(pluginManager.plugins); i++ {
		currentPlugin := pluginManager.plugins[i]
		pluginSubscribedMethods := currentPlugin.GetSubscribedMethods()
		isSubscribed := slices.Contains(pluginSubscribedMethods, strategy) || slices.Contains(pluginSubscribedMethods, ALL_METHODS)

		if isSubscribed && currentPlugin.AcceptsStrategy(strategy) {
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
	hosts []*host_info_util.HostInfo) (host *host_info_util.HostInfo, err error) {
	for i := 0; i < len(pluginManager.plugins); i++ {
		currentPlugin := pluginManager.plugins[i]
		isSubscribed := slices.Contains(currentPlugin.GetSubscribedMethods(), ALL_METHODS) || slices.Contains(currentPlugin.GetSubscribedMethods(), GET_HOST_INFO_BY_STRATEGY_METHOD)

		if isSubscribed {
			host, err = currentPlugin.GetHostInfoByStrategy(role, strategy, hosts)

			if err == nil {
				return
			}
		}
	}

	if err == nil {
		err = error_util.NewUnsupportedStrategyError(
			error_util.GetMessage("PluginManagerImpl.unsupportedHostSelectionStrategy", strategy))
	}
	return
}

func (pluginManager *PluginManagerImpl) GetHostSelectorStrategy(strategy string) (hostSelector driver_infrastructure.HostSelector, err error) {
	for i := 0; i < len(pluginManager.plugins); i++ {
		currentPlugin := pluginManager.plugins[i]
		isSubscribed := slices.Contains(currentPlugin.GetSubscribedMethods(), ALL_METHODS) || slices.Contains(currentPlugin.GetSubscribedMethods(), GET_HOST_SELECT_STRATEGY_METHOD)

		if isSubscribed {
			hostSelector, err = currentPlugin.GetHostSelectorStrategy(strategy)
			if err == nil {
				return
			}
		}
	}

	if err == nil {
		err = error_util.NewUnsupportedStrategyError(
			error_util.GetMessage("PluginManagerImpl.unsupportedHostSelectionStrategy", strategy))
	}
	return
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
	slog.Debug(error_util.GetMessage("PluginManagerImpl.releaseResources"))

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

func (pluginManager *PluginManagerImpl) GetTelemetryContext() context.Context {
	pluginManager.telemetryCtxLock.Lock()
	defer pluginManager.telemetryCtxLock.Unlock()
	return pluginManager.telemetryCtx
}

func (pluginManager *PluginManagerImpl) GetTelemetryFactory() telemetry.TelemetryFactory {
	return pluginManager.telemetryFactory
}

func (pluginManager *PluginManagerImpl) SetTelemetryContext(ctx context.Context) {
	pluginManager.telemetryCtxLock.Lock()
	defer pluginManager.telemetryCtxLock.Unlock()
	pluginManager.telemetryCtx = ctx
}
