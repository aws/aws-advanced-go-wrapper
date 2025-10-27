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

package plugins

import (
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/connection_tracker"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

type AuroraConnectionTrackerPluginFactory struct{}

func (factory AuroraConnectionTrackerPluginFactory) GetInstance(pluginService driver_infrastructure.PluginService,
	props *utils.RWMap[string, string]) (driver_infrastructure.ConnectionPlugin, error) {
	return NewAuroraConnectionTrackerPlugin(pluginService, props, connection_tracker.NewOpenedConnectionTracker(pluginService)), nil
}

func (factory AuroraConnectionTrackerPluginFactory) ClearCaches() {}

func NewAuroraConnectionTrackerPluginFactory() driver_infrastructure.ConnectionPluginFactory {
	return AuroraConnectionTrackerPluginFactory{}
}

type AuroraConnectionTrackerPlugin struct {
	BaseConnectionPlugin
	pluginService           driver_infrastructure.PluginService
	tracker                 connection_tracker.ConnectionTracker
	props                   *utils.RWMap[string, string]
	currentWriter           *host_info_util.HostInfo
	needUpdateCurrentWriter bool
}

func NewAuroraConnectionTrackerPlugin(
	pluginService driver_infrastructure.PluginService,
	props *utils.RWMap[string, string],
	tracker connection_tracker.ConnectionTracker) *AuroraConnectionTrackerPlugin {
	return &AuroraConnectionTrackerPlugin{
		pluginService: pluginService,
		props:         props,
		tracker:       tracker,
	}
}

func (a AuroraConnectionTrackerPlugin) GetPluginCode() string {
	return driver_infrastructure.AURORA_CONNECTION_TRACKER_PLUGIN_CODE
}

func (a AuroraConnectionTrackerPlugin) GetSubscribedMethods() []string {
	return append([]string{plugin_helpers.CONNECT_METHOD,
		utils.CONN_CLOSE,
		plugin_helpers.NOTIFY_HOST_LIST_CHANGED_METHOD,
		plugin_helpers.NOTIFY_CONNECTION_CHANGED_METHOD},
		utils.NETWORK_BOUND_METHODS...)
}

func (a *AuroraConnectionTrackerPlugin) Execute(
	_ driver.Conn,
	_ string,
	executeFunc driver_infrastructure.ExecuteFunc,
	_ ...any) (wrappedReturnValue any, wrappedReturnValue2 any, wrappedOk bool, wrappedErr error) {
	a.rememberWriter()

	wrappedReturnValue, wrappedReturnValue2, wrappedOk, wrappedErr = executeFunc()
	if wrappedErr != nil {
		var awsWrapperError *error_util.AwsWrapperError
		ok := errors.As(wrappedErr, &awsWrapperError)
		if ok && awsWrapperError.IsFailoverErrorType() {
			a.checkWriterChanged()
		}
		return
	}

	return
}

func (a *AuroraConnectionTrackerPlugin) Connect(
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	_ bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	conn, err := connectFunc(props)

	if conn != nil {
		rdsUrlType := utils.IdentifyRdsUrlType(hostInfo.GetHost())

		if rdsUrlType.IsRdsCluster || rdsUrlType == utils.OTHER || rdsUrlType == utils.IP_ADDRESS {
			hostInfo.ResetAliases()
			a.pluginService.FillAliases(conn, hostInfo)
		}
		a.tracker.PopulateOpenedConnectionQueue(hostInfo, conn)
	}

	return conn, err
}

func (a *AuroraConnectionTrackerPlugin) NotifyHostListChanged(changes map[string]map[driver_infrastructure.HostChangeOptions]bool) {
	for host, changes := range changes {
		if changes[driver_infrastructure.PROMOTED_TO_READER] {
			a.tracker.InvalidateAllConnectionsMultipleHosts(host)
		}
		if changes[driver_infrastructure.PROMOTED_TO_WRITER] {
			a.tracker.InvalidateAllConnectionsMultipleHosts(host)
		}
	}

	a.tracker.LogOpenedConnections()
	a.needUpdateCurrentWriter = true
}

func (a *AuroraConnectionTrackerPlugin) rememberWriter() {
	if a.currentWriter == nil || a.needUpdateCurrentWriter {
		a.currentWriter = host_info_util.GetWriter(a.pluginService.GetHosts())
		a.needUpdateCurrentWriter = false
	}
}

func (a *AuroraConnectionTrackerPlugin) checkWriterChanged() {
	fmt.Println("Check if writer changed!!")
	hostInfoAfterFailover := host_info_util.GetWriter(a.pluginService.GetHosts())
	if hostInfoAfterFailover == nil {
		return
	}

	if a.currentWriter == nil {
		a.currentWriter = hostInfoAfterFailover
		a.needUpdateCurrentWriter = false
	} else if a.currentWriter.GetHostAndPort() != hostInfoAfterFailover.GetHostAndPort() {
		// writer has changed
		a.tracker.InvalidateAllConnections(a.currentWriter)
		a.tracker.LogOpenedConnections()
		a.currentWriter = hostInfoAfterFailover
		a.needUpdateCurrentWriter = false
	}
}

func (a *AuroraConnectionTrackerPlugin) ClearCaches() {
	a.tracker.PruneNullConnections()
}
