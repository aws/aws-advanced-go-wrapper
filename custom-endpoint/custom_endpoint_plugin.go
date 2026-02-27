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
	"time"

	auth_helpers "github.com/aws/aws-advanced-go-wrapper/auth-helpers"
	awssql "github.com/aws/aws-advanced-go-wrapper/awssql/driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
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
const TELEMETRY_ENDPOINT_INFO_CHANGED = "customEndpoint.infoChanged.counter"

type CustomEndpointPluginFactory struct{}

type getRdsClientFunc func(*host_info_util.HostInfo, *utils.RWMap[string, string]) (*rds.Client, error)

func (factory CustomEndpointPluginFactory) GetInstance(
	servicesContainer driver_infrastructure.ServicesContainer,
	props *utils.RWMap[string, string]) (driver_infrastructure.ConnectionPlugin, error) {
	return NewCustomEndpointPlugin(servicesContainer, getRdsClientFuncImpl, props)
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
		return nil, err
	}

	rdsClient := rds.NewFromConfig(cfg)
	return rdsClient, nil
}

func (factory CustomEndpointPluginFactory) ClearCaches() {
	// Monitors are now managed by the MonitorService, so we don't need to clear them here.
	// The MonitorService will handle cleanup when ReleaseResources is called.
	ClearCache()
}

func NewCustomEndpointPluginFactory() driver_infrastructure.ConnectionPluginFactory {
	return CustomEndpointPluginFactory{}
}

type CustomEndpointPlugin struct {
	plugins.BaseConnectionPlugin
	servicesContainer          driver_infrastructure.ServicesContainer
	pluginService              driver_infrastructure.PluginService
	monitorService             driver_infrastructure.MonitorService
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
	servicesContainer driver_infrastructure.ServicesContainer,
	rdsClientFunc getRdsClientFunc,
	props *utils.RWMap[string, string]) (*CustomEndpointPlugin, error) {
	pluginService := servicesContainer.GetPluginService()
	monitorService := servicesContainer.GetMonitorService()

	waitForInfoCounter, err := pluginService.GetTelemetryFactory().CreateCounter(TELEMETRY_WAIT_FOR_INFO_COUNTER)
	if err != nil {
		return nil, err
	}

	idleMonitorExpirationMs := property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.CUSTOM_ENDPOINT_MONITOR_IDLE_EXPIRATION_MS)

	// Register the monitor type with the monitor service
	monitorService.RegisterMonitorType(
		CustomEndpointMonitorType,
		&driver_infrastructure.MonitorSettings{
			ExpirationTimeout: time.Millisecond * time.Duration(idleMonitorExpirationMs),
			InactiveTimeout:   1 * time.Minute,
			ErrorResponses:    map[driver_infrastructure.MonitorErrorResponse]bool{driver_infrastructure.MonitorErrorRecreate: true},
		},
		"", // No produced data type
	)

	return &CustomEndpointPlugin{
		servicesContainer:          servicesContainer,
		pluginService:              pluginService,
		monitorService:             monitorService,
		props:                      props,
		shouldWaitForInfo:          property_util.GetVerifiedWrapperPropertyValue[bool](props, property_util.WAIT_FOR_CUSTOM_ENDPOINT_INFO),
		waitOnCachedInfoDurationMs: property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.WAIT_FOR_CUSTOM_ENDPOINT_INFO_TIMEOUT_MS),
		idleMonitorExpirationMs:    idleMonitorExpirationMs,
		waitForInfoCounter:         waitForInfoCounter,
		rdsClientFunc:              rdsClientFunc,
	}, nil
}

// NOTE: This method is for testing purposes.
func NewCustomEndpointPluginWithHostInfo(
	servicesContainer driver_infrastructure.ServicesContainer,
	rdsClientFunc getRdsClientFunc,
	props *utils.RWMap[string, string],
	customEndpointHostInfo *host_info_util.HostInfo) (*CustomEndpointPlugin, error) {
	plugin, err := NewCustomEndpointPlugin(servicesContainer, rdsClientFunc, props)
	if err != nil {
		return nil, err
	}
	plugin.customEndpointHostInfo = customEndpointHostInfo
	return plugin, nil
}

func (plugin *CustomEndpointPlugin) GetPluginCode() string {
	return driver_infrastructure.CUSTOM_ENDPOINT_PLUGIN_CODE
}

func (plugin *CustomEndpointPlugin) GetSubscribedMethods() []string {
	return append([]string{
		"Connect",
	}, utils.NETWORK_BOUND_METHODS...)
}

func (plugin *CustomEndpointPlugin) Connect(
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	_ bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	if !utils.IsRdsCustomClusterDns(hostInfo.GetHost()) {
		return connectFunc(props)
	}

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

	// Capture values for the initializer closure
	customEndpointHostInfo := plugin.customEndpointHostInfo
	endpointIdentifier := plugin.customEndpointId
	region := plugin.region
	rdsClientFunc := plugin.rdsClientFunc
	propsCopy := plugin.props

	monitor, err := plugin.monitorService.RunIfAbsent(
		CustomEndpointMonitorType,
		customEndpointHostInfo.Host,
		plugin.servicesContainer,
		func(container driver_infrastructure.ServicesContainer) (driver_infrastructure.Monitor, error) {
			rdsClient, err := rdsClientFunc(customEndpointHostInfo, propsCopy)
			if err != nil {
				return nil, err
			}
			infoChangedCounter, err := container.GetPluginService().GetTelemetryFactory().CreateCounter(TELEMETRY_ENDPOINT_INFO_CHANGED)
			if err != nil {
				return nil, err
			}

			return NewCustomEndpointMonitorImpl(
				container,
				customEndpointHostInfo,
				endpointIdentifier,
				region,
				refreshRateMs,
				infoChangedCounter,
				rdsClient,
			), nil
		},
	)
	if err != nil {
		return nil, err
	}

	// Type assert to CustomEndpointMonitor
	customEndpointMonitor, ok := monitor.(CustomEndpointMonitor)
	if !ok {
		return nil, errors.New("monitor is not a CustomEndpointMonitor")
	}
	return customEndpointMonitor, nil
}

func (plugin *CustomEndpointPlugin) waitForCustomEndpointInfo(monitor CustomEndpointMonitor) error {
	hasCustomEndpointInfo := monitor.HasCustomEndpointInfo()

	if !hasCustomEndpointInfo {
		monitor.RequestCustomEndpointInfoUpdate()

		if plugin.waitForInfoCounter != nil {
			plugin.waitForInfoCounter.Inc(plugin.pluginService.GetTelemetryContext())
		}

		waitForEndpointInfoTimeout := time.Now().Add(time.Millisecond * time.Duration(plugin.waitOnCachedInfoDurationMs))
		for !hasCustomEndpointInfo && time.Now().Before(waitForEndpointInfoTimeout) {
			time.Sleep(time.Millisecond * time.Duration(100))
			hasCustomEndpointInfo = monitor.HasCustomEndpointInfo()
		}

		if !hasCustomEndpointInfo {
			return errors.New(error_util.GetMessage("CustomEndpointPlugin.timedOutWaitingForCustomEndpointInfo"))
		}
	}
	return nil
}
