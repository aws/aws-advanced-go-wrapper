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

package bg

import (
	"database/sql/driver"
	"slices"
	"sync/atomic"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

var bgSubscribedMethods = append(utils.NETWORK_BOUND_METHODS, plugin_helpers.CONNECT_METHOD)
var providers = utils.NewRWMapWithDisposalFunc(func(provider *BlueGreenStatusProvider) bool {
	if provider != nil {
		provider.ClearMonitors()
	}
	return true
})

type BlueGreenPluginFactory struct{}

func (b *BlueGreenPluginFactory) ClearCaches() {
	providers.Clear()
}

func (b *BlueGreenPluginFactory) GetInstance(pluginService driver_infrastructure.PluginService, props map[string]string) (driver_infrastructure.ConnectionPlugin, error) {
	return NewBlueGreenPlugin(pluginService, props)
}

func NewBlueGreenPluginFactory() driver_infrastructure.ConnectionPluginFactory {
	return &BlueGreenPluginFactory{}
}

type BlueGreenPlugin struct {
	bgId               string
	bgStatus           driver_infrastructure.BlueGreenStatus
	bgProviderSupplier BlueGreenProviderSupplier
	isIamInUse         bool
	pluginService      driver_infrastructure.PluginService
	props              map[string]string
	startTime          atomic.Int64
	endTime            atomic.Int64
	plugins.BaseConnectionPlugin
}

func NewBlueGreenPlugin(pluginService driver_infrastructure.PluginService,
	props map[string]string) (driver_infrastructure.ConnectionPlugin, error) {
	bgId := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.BGD_ID)
	if bgId == "" {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("BlueGreenDeployment.bgIdRequired"))
	}
	return &BlueGreenPlugin{
		bgId:               bgId,
		props:              props,
		pluginService:      pluginService,
		bgProviderSupplier: NewBlueGreenStatusProvider,
	}, nil
}

func (b *BlueGreenPlugin) GetPluginCode() string {
	return driver_infrastructure.BLUE_GREEN_PLUGIN_CODE
}

func (b *BlueGreenPlugin) GetSubscribedMethods() []string {
	return bgSubscribedMethods
}

func (b *BlueGreenPlugin) Connect(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (conn driver.Conn, err error) {
	b.resetRoutingTimeNano()
	defer func() {
		if b.startTime.Load() > 0 {
			b.endTime.CompareAndSwap(0, time.Now().Unix())
		}
	}()

	bgStatus, ok := b.pluginService.GetBgStatus(b.bgId)
	b.bgStatus = bgStatus

	if b.bgStatus.IsZero() || !ok {
		// Connection does not require BG logic.
		return b.regularOpenConnection(connectFunc, isInitialConnection)
	}

	if isInitialConnection {
		// Upon initial connection, mark whether iam is in use.
		b.isIamInUse = b.pluginService.IsPluginInUse(driver_infrastructure.IAM_PLUGIN_CODE)
	}

	hostRole, ok := b.bgStatus.GetRole(hostInfo)

	if !ok || hostRole.IsZero() {
		// Connection to a host that is not participating in BG switchover.
		return b.regularOpenConnection(connectFunc, isInitialConnection)
	}

	matchingRoutes := utils.FilterSlice(b.bgStatus.GetConnectRoutings(), func(r driver_infrastructure.ConnectRouting) bool {
		return r.IsMatch(hostInfo, hostRole)
	})

	if len(matchingRoutes) == 0 {
		return b.regularOpenConnection(connectFunc, isInitialConnection)
	}

	b.startTime.Store(time.Now().UnixNano())
	routing := matchingRoutes[0]
	for routing != nil && conn == nil {
		conn, err = routing.Apply(b, hostInfo, props, isInitialConnection, b.pluginService)
		if conn == nil {
			b.bgStatus, ok = b.pluginService.GetBgStatus(b.bgId)
			if !b.bgStatus.IsZero() && ok {
				matchingRoutes := utils.FilterSlice(b.bgStatus.GetConnectRoutings(), func(r driver_infrastructure.ConnectRouting) bool {
					return r.IsMatch(hostInfo, hostRole)
				})

				if len(matchingRoutes) != 0 {
					routing = matchingRoutes[0]
					continue
				}
			}
			routing = nil
		}
	}

	if conn == nil {
		conn, err = connectFunc(props)
	}

	if isInitialConnection {
		// Provider should be initialized after connection is open and a dialect is properly identified.
		b.initProvider()
	}
	return conn, err
}

func (b *BlueGreenPlugin) Execute(
	_ driver.Conn,
	methodName string,
	executeFunc driver_infrastructure.ExecuteFunc,
	methodArgs ...any) (wrappedReturnValue any, wrappedReturnValue2 any, wrappedOk bool, wrappedErr error) {
	b.resetRoutingTimeNano()
	defer func() {
		if b.startTime.Load() > 0 {
			b.endTime.CompareAndSwap(0, time.Now().Unix())
		}
	}()
	b.initProvider()
	if slices.Contains(utils.CLOSING_METHODS, methodName) {
		return executeFunc()
	}
	bgStatus, ok := b.pluginService.GetBgStatus(b.bgId)
	b.bgStatus = bgStatus
	if b.bgStatus.IsZero() || !ok {
		return executeFunc()
	}
	currentHostInfo, err := b.pluginService.GetCurrentHostInfo()
	hostRole, ok := b.bgStatus.GetRole(currentHostInfo)
	if err != nil || !ok || hostRole.IsZero() {
		return executeFunc()
	}

	matchingRoutes := utils.FilterSlice(b.bgStatus.GetExecuteRoutings(), func(r driver_infrastructure.ExecuteRouting) bool {
		return r.IsMatch(currentHostInfo, hostRole)
	})

	if len(matchingRoutes) == 0 {
		return executeFunc()
	}
	b.startTime.Store(time.Now().UnixNano())
	routing := matchingRoutes[0]
	result := driver_infrastructure.EMPTY_ROUTING_RESULT_HOLDER
	for routing != nil && !result.IsPresent() {
		result = routing.Apply(b, b.props, b.pluginService, methodName, executeFunc, methodArgs...)
		if !result.IsPresent() {
			b.bgStatus, ok = b.pluginService.GetBgStatus(b.bgId)
			if b.bgStatus.IsZero() || !ok {
				b.endTime.Store(time.Now().UnixNano())
				return executeFunc()
			}
			matchingRoutes := utils.FilterSlice(b.bgStatus.GetExecuteRoutings(), func(r driver_infrastructure.ExecuteRouting) bool {
				return r.IsMatch(currentHostInfo, hostRole)
			})

			if len(matchingRoutes) != 0 {
				routing = matchingRoutes[0]
				continue
			}
			routing = nil
		}
	}

	b.endTime.Store(time.Now().UnixNano())
	if result.IsPresent() {
		return result.GetResult()
	}
	return executeFunc()
}

func (b *BlueGreenPlugin) initProvider() {
	provider, ok := providers.Get(b.bgId)
	if !ok || provider.isZero() {
		provider = b.bgProviderSupplier(b.pluginService, b.props, b.bgId)
		providers.Put(b.bgId, provider)
	}
}

func (b *BlueGreenPlugin) regularOpenConnection(connectFunc driver_infrastructure.ConnectFunc, isInitialConnection bool) (driver.Conn, error) {
	conn, err := connectFunc(b.props)
	if isInitialConnection {
		// Provider should be initialized after connection is open and a dialect is properly identified.
		b.initProvider()
	}
	return conn, err
}

// For testing purposes only.
func (b *BlueGreenPlugin) GetHoldTimeNano() time.Duration {
	if b.startTime.Load() == 0 {
		return 0 * time.Nanosecond
	}
	if b.endTime.Load() == 0 {
		return time.Duration(time.Now().Unix()-b.startTime.Load()) * time.Nanosecond
	}
	return time.Duration(b.endTime.Load()-b.startTime.Load()) * time.Nanosecond
}

func (b *BlueGreenPlugin) resetRoutingTimeNano() {
	b.startTime.Store(0)
	b.endTime.Store(0)
}
