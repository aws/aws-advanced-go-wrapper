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

package custom_endpoint

import (
	"context"
	"database/sql/driver"
	"errors"
	"log/slog"
	"time"

	auth_helpers "github.com/aws/aws-advanced-go-wrapper/auth-helpers"
	awssql "github.com/aws/aws-advanced-go-wrapper/awssql/driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/region_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/rds"
)

func init() {
	awssql.UsePluginFactory(driver_infrastructure.CUSTOM_ENDPOINT_PLUGIN_CODE,
		NewCustomEndpointPluginFactory())
}

const TELEMETRY_WAIT_FOR_INFO_COUNTER = "customEndpoint.waitForInfo.counter"

type CustomEndpointPluginFactory struct{}

type getRdsClientFunc func(*host_info_util.HostInfo, *utils.RWMap[string, string]) (*rds.Client, error)

func (factory CustomEndpointPluginFactory) GetInstance(
	pluginService driver_infrastructure.PluginService,
	props *utils.RWMap[string, string]) (driver_infrastructure.ConnectionPlugin, error) {

	return NewCustomEndpointPlugin(pluginService, getRdsClientFuncImpl, props)
}

func getRdsClientFuncImpl(hostInfo *host_info_util.HostInfo, props *utils.RWMap[string, string]) (*rds.Client, error) {
	region := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.CUSTOM_ENDPOINT_REGION_PROPERTY)

	awsCredentialsProvider, err := auth_helpers.GetAwsCredentialsProvider(*hostInfo, props.GetAllEntries())
	if err != nil {
		return nil, err
	}

	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion(region),
		config.WithCredentialsProvider(awsCredentialsProvider))
	if err != nil {
		slog.Error("Failed to load AWS configuration", "error", err)
	}

	rdsClient := rds.NewFromConfig(cfg)
	return rdsClient, nil
}

func (factory CustomEndpointPluginFactory) ClearCaches() {}

func NewCustomEndpointPluginFactory() driver_infrastructure.ConnectionPluginFactory {
	return CustomEndpointPluginFactory{}
}

var monitorDisposalFunc utils.DisposalFunc[CustomEndpointMonitor] = func(item CustomEndpointMonitor) bool {
	item.Close()
	return true
}
var monitors = utils.NewSlidingExpirationCache[CustomEndpointMonitor](
	"custom-endpoint-monitor", monitorDisposalFunc)

type CustomEndpointPlugin struct {
	plugins.BaseConnectionPlugin
	pluginService              driver_infrastructure.PluginService
	props                      *utils.RWMap[string, string]
	shouldWaitForInfo          bool
	waitOnCachedInfoDurationMs int
	idleMonitorExpirationMs    int
	waitForInfoCounter         telemetry.TelemetryCounter
	customEndpointHostInfo     *host_info_util.HostInfo
	customEndpointId           string
	region                     region_util.Region
	rdsClientFunc              getRdsClientFunc
}

func NewCustomEndpointPlugin(
	pluginService driver_infrastructure.PluginService,
	rdsClientFunc getRdsClientFunc,
	props *utils.RWMap[string, string]) (*CustomEndpointPlugin, error) {

	waitForInfoCounter, err := pluginService.GetTelemetryFactory().CreateCounter(TELEMETRY_WAIT_FOR_INFO_COUNTER)
	if err != nil {
		return nil, err
	}

	return &CustomEndpointPlugin{
		pluginService:              pluginService,
		props:                      props,
		shouldWaitForInfo:          property_util.GetVerifiedWrapperPropertyValue[bool](props, property_util.WAIT_FOR_CUSTOM_ENDPOINT_INFO),
		waitOnCachedInfoDurationMs: property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.WAIT_FOR_CUSTOM_ENDPOINT_INFO_TIMEOUT_MS),
		idleMonitorExpirationMs:    property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.CUSTOM_ENDPOINT_MONITOR_IDLE_EXPIRATION_MS),
		waitForInfoCounter:         waitForInfoCounter,
		rdsClientFunc:              rdsClientFunc,
	}, nil
}

func (plugin *CustomEndpointPlugin) GetSubscribedMethods() []string {
	return append([]string{
		plugin_helpers.CONNECT_METHOD,
	}, utils.NETWORK_BOUND_METHODS...)
}

func (plugin *CustomEndpointPlugin) Connect(
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {

	plugin.customEndpointHostInfo = hostInfo
	plugin.customEndpointId = utils.GetRdsClusterId(hostInfo.GetHost())
	if plugin.customEndpointId == "" {
		return nil, errors.New(error_util.GetMessage("CustomEndpointPlugin.errorParsingEndpointIdentifier", hostInfo.GetHost()))
	}

	plugin.region = region_util.GetRegion(hostInfo.GetHost(), props, property_util.CUSTOM_ENDPOINT_REGION_PROPERTY)
	if plugin.region == "" {
		return nil, errors.New(error_util.GetMessage("CustomEndpointPlugin.unableToDetermineRegion", property_util.CUSTOM_ENDPOINT_REGION_PROPERTY.Name))
	}

	monitor, err := plugin.createMonitorIfAbsent(props)
	if err != nil {
		return nil, err
	}

	if plugin.shouldWaitForInfo {
		err := plugin.waitForCustomEndpointInfo(monitor)
		if err != nil {
			return nil, err
		}
	}

	return connectFunc(props)
}

func (plugin *CustomEndpointPlugin) Execute(
	_ driver.Conn,
	_ string,
	executeFunc driver_infrastructure.ExecuteFunc,
	_ ...any) (wrappedReturnValue any, wrappedReturnValue2 any, wrappedOk bool, wrappedErr error) {
	if plugin.customEndpointHostInfo == nil {
		return executeFunc()
	}

	monitor, err := plugin.createMonitorIfAbsent(plugin.props)
	if err != nil {
		return nil, nil, false, err
	}
	if plugin.shouldWaitForInfo {
		err := plugin.waitForCustomEndpointInfo(monitor)
		if err != nil {
			return nil, nil, false, err
		}
	}

	return executeFunc()
}

func (plugin *CustomEndpointPlugin) createMonitorIfAbsent(
	props *utils.RWMap[string, string]) (CustomEndpointMonitor, error) {
	refreshRateMs := time.Millisecond * time.Duration(property_util.GetRefreshRateValue(props, property_util.CUSTOM_ENDPOINT_INFO_REFRESH_RATE_MS))
	return monitors.ComputeIfAbsentWithError(
		plugin.customEndpointHostInfo.Host,
		func() (CustomEndpointMonitor, error) {
			rdsClient, err := plugin.rdsClientFunc(plugin.customEndpointHostInfo, plugin.props)
			if err != nil {
				return nil, err
			}
			return NewCustomEndpointMonitorImpl(
				plugin.pluginService,
				plugin.customEndpointHostInfo,
				plugin.customEndpointId,
				plugin.region,
				refreshRateMs,
				rdsClient,
			), nil
		}, 1)
}

func (plugin *CustomEndpointPlugin) waitForCustomEndpointInfo(monitor CustomEndpointMonitor) error {
	hasCustomEdnpointInfo := monitor.HasCustomEndpointInfo()

	if !hasCustomEdnpointInfo {
		if plugin.waitForInfoCounter != nil {
			plugin.waitForInfoCounter.Inc(plugin.pluginService.GetTelemetryContext())
		}

		waitForEndpointInfoTimeout := time.Now().Add(time.Millisecond * time.Duration(plugin.waitOnCachedInfoDurationMs))
		for !hasCustomEdnpointInfo && time.Now().Before(waitForEndpointInfoTimeout) {
			time.Sleep(time.Millisecond * time.Duration(100))
			hasCustomEdnpointInfo = monitor.HasCustomEndpointInfo()
		}

		if !hasCustomEdnpointInfo {
			return errors.New(error_util.GetMessage("CustomEndpointPlugin.timedOutWaitingForCustomEndpointInfo"))
		}
	}
	return nil
}
