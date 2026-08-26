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
	"sync"
	"time"

	auth_helpers "github.com/aws/aws-advanced-go-wrapper/auth-helpers"
	awssql "github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/plugins"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/region_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils/telemetry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/rds"
)

func init() {
	awssql.UsePluginFactory(driver_infrastructure.CUSTOM_ENDPOINT_PLUGIN_CODE,
		NewCustomEndpointPluginFactory())
}

// roleFilteringNotice keeps the deprecated-default warning to one line per process.
var roleFilteringNotice sync.Once

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
	// GetRegion, not the property alone: the property is "" when unset, config.WithRegion("") is ignored by
	// the AWS SDK, and the region then falls back to AWS_REGION rather than to the endpoint's own hostname.
	region := string(region_util.GetRegion(hostInfo.GetHost(), props, property_util.CUSTOM_ENDPOINT_REGION_PROPERTY))

	awsCredentialsProvider, err := auth_helpers.GetAwsCredentialsProvider(*hostInfo, props.GetAllEntries())
	if err != nil {
		return nil, err
	}

	// The deadline bounds credential resolution as well as the config load: a wedged SSO or STS chain
	// would otherwise block the connect path indefinitely, which is the one thing RDS_FETCH_TIMEOUT
	// cannot protect against because it only covers calls made after the client exists.
	ctx, cancel := context.WithTimeout(context.Background(), RDS_FETCH_TIMEOUT)
	defer cancel()

	cfg, err := config.LoadDefaultConfig(
		ctx,
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
	props                      *utils.RWMap[string, string]
	shouldWaitForInfo          bool
	waitOnCachedInfoDurationMs int
	idleMonitorExpirationMs    int
	refreshRate                time.Duration
	maxRefreshRate             time.Duration
	backoffFactor              int
	enforceRoleFiltering       bool
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

	// Parsed once, not per call: Execute runs on every network-bound method including Rows.Next, so props
	// reads inside it are paid per row. Not via GetRefreshRateValue, whose error message does not apply to
	// this plugin.
	refreshRate := time.Millisecond * time.Duration(
		property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.CUSTOM_ENDPOINT_INFO_REFRESH_RATE_MS))
	maxRefreshRate := time.Millisecond * time.Duration(
		property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.CUSTOM_ENDPOINT_INFO_MAX_REFRESH_RATE_MS))
	backoffFactor := property_util.GetVerifiedWrapperPropertyValue[int](
		props, property_util.CUSTOM_ENDPOINT_INFO_REFRESH_RATE_BACKOFF_FACTOR)
	enforceRoleFiltering := property_util.GetVerifiedWrapperPropertyValue[bool](
		props, property_util.CUSTOM_ENDPOINT_ENFORCE_ROLE_FILTERING)

	// The monitor service treats silence longer than InactiveTimeout as a fault and recreates the
	// monitor. The monitor now stamps liveness from inside its sleeps, so the five-minute pause it
	// takes after an authorization failure no longer reads as a wedged monitor, and this timeout no
	// longer has to track it. It only has to exceed the longest the monitor can genuinely be
	// unresponsive, which is one bounded RDS call.
	inactiveTimeout := max(1*time.Minute, RDS_FETCH_TIMEOUT+30*time.Second)

	// Register the monitor type with the monitor service
	monitorService.RegisterMonitorType(
		CustomEndpointMonitorType,
		&driver_infrastructure.MonitorSettings{
			ExpirationTimeout: time.Millisecond * time.Duration(idleMonitorExpirationMs),
			InactiveTimeout:   inactiveTimeout,
			ErrorResponses:    map[driver_infrastructure.MonitorErrorResponse]bool{driver_infrastructure.MonitorErrorRecreate: true},
		},
		"", // No produced data type
	)

	return &CustomEndpointPlugin{
		servicesContainer:          servicesContainer,
		props:                      props,
		shouldWaitForInfo:          property_util.GetVerifiedWrapperPropertyValue[bool](props, property_util.WAIT_FOR_CUSTOM_ENDPOINT_INFO),
		waitOnCachedInfoDurationMs: property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.WAIT_FOR_CUSTOM_ENDPOINT_INFO_TIMEOUT_MS),
		idleMonitorExpirationMs:    idleMonitorExpirationMs,
		refreshRate:                refreshRate,
		maxRefreshRate:             maxRefreshRate,
		backoffFactor:              backoffFactor,
		enforceRoleFiltering:       enforceRoleFiltering,
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

// `Tx.Rollback`, `Conn.IsValid` and `Conn.ResetSession` are intentionally left unsubscribed
// since they require no custom endpoint information.
var subscribedMethods = func() []string {
	ungated := map[string]bool{
		utils.TX_ROLLBACK:        true,
		utils.CONN_IS_VALID:      true,
		utils.CONN_RESET_SESSION: true,
	}
	methods := []string{plugin_helpers.CONNECT_METHOD}
	for _, method := range utils.NETWORK_BOUND_METHODS {
		if !ungated[method] {
			methods = append(methods, method)
		}
	}
	return methods
}()

func (plugin *CustomEndpointPlugin) GetSubscribedMethods() []string {
	return subscribedMethods
}

func (plugin *CustomEndpointPlugin) Connect(
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	_ bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	if !utils.IsRdsCustomClusterDns(hostInfo.GetHost()) {
		return connectFunc(props)
	}

	slog.Debug(error_util.GetMessage("CustomEndpointPlugin.connectionRequestToCustomEndpoint",
		hostInfo.GetUrl()))

	// Once per process, not per connection: a plugin instance is created per connection, so warning from
	// the instance would repeat for every one of them.
	if !plugin.enforceRoleFiltering {
		roleFilteringNotice.Do(func() {
			slog.Warn(error_util.GetMessage("CustomEndpointPlugin.roleFilteringDisabled"))
		})
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
	_ *utils.RWMap[string, string]) (CustomEndpointMonitor, error) {
	// Capture values for the initializer closure
	customEndpointHostInfo := plugin.customEndpointHostInfo
	endpointIdentifier := plugin.customEndpointId
	region := plugin.region
	rdsClientFunc := plugin.rdsClientFunc
	propsCopy := plugin.props
	refreshRate := plugin.refreshRate
	maxRefreshRate := plugin.maxRefreshRate
	backoffFactor := plugin.backoffFactor
	enforceRoleFiltering := plugin.enforceRoleFiltering

	monitor, err := plugin.servicesContainer.GetMonitorService().RunIfAbsent(
		CustomEndpointMonitorType,
		customEndpointHostInfo.GetUrl(),
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
				refreshRate,
				maxRefreshRate,
				backoffFactor,
				enforceRoleFiltering,
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
		slog.Debug(error_util.GetMessage("CustomEndpointPlugin.waitingForCustomEndpointInfo",
			plugin.waitOnCachedInfoDurationMs, plugin.customEndpointHostInfo.GetUrl()))

		if plugin.waitForInfoCounter != nil {
			plugin.waitForInfoCounter.Inc(plugin.servicesContainer.GetPluginService().GetTelemetryContext())
		}

		waitForEndpointInfoTimeout := time.Now().Add(time.Millisecond * time.Duration(plugin.waitOnCachedInfoDurationMs))
		for !hasCustomEndpointInfo && time.Now().Before(waitForEndpointInfoTimeout) {
			time.Sleep(time.Millisecond * time.Duration(100))
			hasCustomEndpointInfo = monitor.HasCustomEndpointInfo()
		}

		if !hasCustomEndpointInfo {
			// The message takes the timeout and the host; both were missing, so the error the
			// application saw read "...after %!v(MISSING) ms ... for host %!s(MISSING)".
			message := error_util.GetMessage("CustomEndpointPlugin.timedOutWaitingForCustomEndpointInfo",
				plugin.waitOnCachedInfoDurationMs, plugin.customEndpointHostInfo.GetUrl())
			// Logged as well as returned: this fires on connect and on every statement, and an application
			// that retries on error would otherwise show an operator nothing.
			slog.Warn(message)
			return errors.New(message)
		}
	}
	return nil
}
