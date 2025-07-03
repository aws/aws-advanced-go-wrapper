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

package efm

import (
	"database/sql/driver"
	"log/slog"
	"slices"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

type HostMonitoringPluginFactory struct {
}

func (h HostMonitoringPluginFactory) GetInstance(pluginService driver_infrastructure.PluginService,
	properties map[string]string) (driver_infrastructure.ConnectionPlugin, error) {
	if pluginService == nil {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("HostMonitoringConnectionPlugin.illegalArgumentError", "pluginService"))
	}
	if properties == nil {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("HostMonitoringConnectionPlugin.illegalArgumentError", "properties"))
	}
	failureDetectionTimeMillis, err := property_util.GetPositiveIntProperty(properties, property_util.FAILURE_DETECTION_TIME_MS)
	if err != nil {
		return nil, err
	}
	failureDetectionIntervalMillis, err := property_util.GetPositiveIntProperty(properties, property_util.FAILURE_DETECTION_INTERVAL_MS)
	if err != nil {
		return nil, err
	}
	failureDetectionCount, err := property_util.GetPositiveIntProperty(properties, property_util.FAILURE_DETECTION_COUNT)
	if err != nil {
		return nil, err
	}
	monitorDisposalTimeMillis, err := property_util.GetPositiveIntProperty(properties, property_util.MONITOR_DISPOSAL_TIME_MS)
	if err != nil {
		return nil, err
	}
	return &HostMonitorConnectionPlugin{pluginService: pluginService, props: properties, failureDetectionTimeMs: failureDetectionTimeMillis,
		failureDetectionIntervalMs: failureDetectionIntervalMillis, failureDetectionCount: failureDetectionCount, monitorDisposalTimeMs: monitorDisposalTimeMillis}, nil
}

func (h HostMonitoringPluginFactory) ClearCaches() {
	if EFM_MONITORS != nil {
		EFM_MONITORS.Clear()
	}
}

func NewHostMonitoringPluginFactory() driver_infrastructure.ConnectionPluginFactory {
	return HostMonitoringPluginFactory{}
}

type HostMonitorConnectionPlugin struct {
	pluginService              driver_infrastructure.PluginService
	props                      map[string]string
	monitoringHostInfo         *host_info_util.HostInfo
	monitorService             MonitorService
	failureDetectionTimeMs     int
	failureDetectionIntervalMs int
	failureDetectionCount      int
	monitorDisposalTimeMs      int
	plugins.BaseConnectionPlugin
}

func (b *HostMonitorConnectionPlugin) GetSubscribedMethods() []string {
	return []string{"*"}
}

func (b *HostMonitorConnectionPlugin) Connect(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	conn, err := connectFunc(props)
	if err != nil {
		return nil, err
	}
	if utils.IdentifyRdsUrlType(hostInfo.Host).IsRds {
		hostInfo.ResetAliases()
		b.pluginService.FillAliases(conn, hostInfo)
	}
	return conn, err
}

func (b *HostMonitorConnectionPlugin) NotifyConnectionChanged(changes map[driver_infrastructure.HostChangeOptions]bool) driver_infrastructure.OldConnectionSuggestedAction {
	_, hostNameChanged := changes[driver_infrastructure.HOSTNAME]
	_, hostChanged := changes[driver_infrastructure.HOST_CHANGED]
	if hostNameChanged || hostChanged {
		// Reset monitoringHostInfo as the associated connection has changed.
		b.monitoringHostInfo = nil
	}

	return driver_infrastructure.NO_OPINION
}

func (b *HostMonitorConnectionPlugin) Execute(
	connInvokedOn driver.Conn,
	methodName string,
	executeFunc driver_infrastructure.ExecuteFunc,
	methodArgs ...any) (wrappedReturnValue any, wrappedReturnValue2 any, wrappedOk bool, wrappedErr error) {
	if !slices.Contains(utils.NETWORK_BOUND_METHODS, methodName) {
		return executeFunc()
	}

	err := b.initMonitorService()
	if err != nil {
		return nil, nil, false, err
	}

	// Sets up a MonitorConnectionState that is active for the duration of executeFunc.
	// If there are any issues setting up the monitor/state, the error is passed on in wrappedErr.
	var monitorState *MonitorConnectionState
	monitoringHostInfo, err := b.getMonitoringHostInfo()
	if err == nil {
		slog.Debug(error_util.GetMessage("HostMonitoringConnectionPlugin.activatedMonitoring", methodName))
		monitorState, err = b.monitorService.StartMonitoring(
			b.pluginService.GetCurrentConnectionRef(), monitoringHostInfo, b.props,
			b.failureDetectionTimeMs, b.failureDetectionIntervalMs, b.failureDetectionCount, b.monitorDisposalTimeMs)
		if err != nil {
			slog.Warn(err.Error())
			wrappedErr = err
			return
		}
		wrappedReturnValue, wrappedReturnValue2, wrappedOk, wrappedErr = executeFunc()

		if monitorState != nil {
			b.monitorService.StopMonitoring(monitorState, b.pluginService.GetCurrentConnection())
			slog.Debug(error_util.GetMessage("HostMonitoringConnectionPlugin.monitoringDeactivated", methodName))
		}
	} else {
		slog.Warn(error_util.GetMessage("HostMonitoringConnectionPlugin.errorGettingMonitoringHostInfo", err.Error()))
		wrappedErr = err
	}

	return
}

func (b *HostMonitorConnectionPlugin) initMonitorService() error {
	if b.monitorService == nil {
		monitorService, err := NewMonitorServiceImpl(b.pluginService)
		if err != nil {
			return err
		}
		b.monitorService = monitorService
	}
	return nil
}

func (b *HostMonitorConnectionPlugin) getMonitoringHostInfo() (*host_info_util.HostInfo, error) {
	if b.monitoringHostInfo.IsNil() {
		monitoringHostInfo, err := b.pluginService.GetCurrentHostInfo()
		if err == nil && !monitoringHostInfo.IsNil() {
			if utils.IsRdsDns(monitoringHostInfo.Host) {
				slog.Debug(error_util.GetMessage("HostMonitoringConnectionPlugin.clusterHostInfoRequired"))
				rdsHostMonitoringInfo, err := b.pluginService.IdentifyConnection(b.pluginService.GetCurrentConnection())
				if err != nil {
					return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("HostMonitoringConnectionPlugin.unableToIdentifyConnection", monitoringHostInfo.Host, err.Error()))
				}
				b.monitoringHostInfo = rdsHostMonitoringInfo
			} else {
				b.monitoringHostInfo = monitoringHostInfo
			}
		} else if err != nil {
			return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage(
				"HostMonitoringConnectionPlugin.errorIdentifyingConnection", err.Error()))
		}
		if b.monitoringHostInfo.IsNil() {
			return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("HostMonitoringConnectionPlugin.unableToIdentifyConnection",
				"<nil>", "Given monitoring HostInfo is nil."))
		}
	}
	return b.monitoringHostInfo, nil
}

// For testing purposes only.
func (b *HostMonitorConnectionPlugin) GetMonitoringHostInfo() *host_info_util.HostInfo {
	return b.monitoringHostInfo
}
