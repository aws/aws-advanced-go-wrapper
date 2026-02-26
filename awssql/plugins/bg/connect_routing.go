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
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
)

type RejectConnectRouting struct {
	BaseRouting
}

func (r *RejectConnectRouting) Apply(_ driver_infrastructure.ConnectionPlugin, _ *host_info_util.HostInfo, _ *utils.RWMap[string, string],
	_ bool, _ driver_infrastructure.PluginService) (driver.Conn, error) {
	message := error_util.GetMessage("BlueGreenDeployment.inProgressCantConnect")
	slog.Debug(message)
	return nil, error_util.NewGenericAwsWrapperError(message)
}

func NewRejectConnectRouting(hostAndPort string, role driver_infrastructure.BlueGreenRole) *RejectConnectRouting {
	return &RejectConnectRouting{NewBaseRouting(hostAndPort, role)}
}

type IamSuccessfulConnectFunc = func(string)

type SubstituteConnectRouting struct {
	substituteHostInfo         *host_info_util.HostInfo
	iamHosts                   []*host_info_util.HostInfo
	iamSuccessfulConnectNotify IamSuccessfulConnectFunc
	BaseRouting
}

func (r *SubstituteConnectRouting) Apply(plugin driver_infrastructure.ConnectionPlugin, _ *host_info_util.HostInfo, props *utils.RWMap[string, string],
	_ bool, pluginService driver_infrastructure.PluginService) (driver.Conn, error) {
	if utils.IsIP(r.substituteHostInfo.GetHost()) {
		return pluginService.Connect(r.substituteHostInfo, props, plugin)
	}

	iamInUse := pluginService.IsPluginInUse(driver_infrastructure.IAM_PLUGIN_CODE)
	if !iamInUse {
		return pluginService.Connect(r.substituteHostInfo, props, plugin)
	}

	if len(r.iamHosts) == 0 {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("BlueGreenDeployment.requireIamHost"))
	}

	for _, iamHost := range r.iamHosts {
		if iamHost == nil {
			// Skip nil entries.
			continue
		}
		var reroutedHostInfo *host_info_util.HostInfo
		if r.substituteHostInfo.GetHost() == "" {
			reroutedHostInfo, _ = host_info_util.NewHostInfoBuilder().SetHost(iamHost.Host).SetHostId(iamHost.HostId).SetAvailability(host_info_util.AVAILABLE).Build()
		} else {
			reroutedHostInfo, _ = host_info_util.NewHostInfoBuilder().CopyFrom(r.substituteHostInfo).SetHostId(iamHost.HostId).SetAvailability(host_info_util.AVAILABLE).Build()
			reroutedHostInfo.AddAlias(iamHost.GetHost())
		}
		rerouteProps := utils.NewRWMapFromCopy(props)
		rerouteProps.Put(property_util.IAM_HOST.Name, iamHost.GetHost())
		if iamHost.IsPortSpecified() {
			rerouteProps.Put(property_util.IAM_DEFAULT_PORT.Name, strconv.Itoa(iamHost.Port))
		}

		conn, err := pluginService.Connect(reroutedHostInfo, rerouteProps, nil)
		if err == nil {
			if r.iamSuccessfulConnectNotify != nil {
				r.iamSuccessfulConnectNotify(iamHost.GetHost())
			}
			return conn, err
		}
	}
	return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("BlueGreenDeployment.inProgressCantOpenConnection", r.substituteHostInfo.GetHostAndPort()))
}

func (r *SubstituteConnectRouting) String() string {
	hostAndPort := "<null>"
	if r.hostAndPort != "" {
		hostAndPort = r.hostAndPort
	}

	role := "<null>"
	if !r.role.IsZero() {
		role = r.role.String()
	}

	substituteHostInfo := "<null>"
	if !r.substituteHostInfo.IsNil() {
		substituteHostInfo = r.substituteHostInfo.GetHostAndPort()
	}

	iamHosts := "<null>"
	if len(r.iamHosts) > 0 {
		hostPorts := make([]string, len(r.iamHosts))
		for i, host := range r.iamHosts {
			hostPorts[i] = host.GetHostAndPort()
		}
		iamHosts = strings.Join(hostPorts, ", ")
	}

	return fmt.Sprintf("%s [%s, %s, substitute: %s, iamHosts: %s]",
		"SubstituteConnectRouting",
		hostAndPort,
		role,
		substituteHostInfo,
		iamHosts,
	)
}

func NewSubstituteConnectRouting(hostAndPort string, role driver_infrastructure.BlueGreenRole, substituteHostInfo *host_info_util.HostInfo,
	iamHosts []*host_info_util.HostInfo, iamSuccessfulConnectNotify IamSuccessfulConnectFunc) *SubstituteConnectRouting {
	return &SubstituteConnectRouting{substituteHostInfo, iamHosts, iamSuccessfulConnectNotify, NewBaseRouting(hostAndPort, role)}
}

type SuspendConnectRouting struct {
	bgId           string
	storageService driver_infrastructure.StorageService
	BaseRouting
}

func (r *SuspendConnectRouting) getBgStatus() *driver_infrastructure.BlueGreenStatus {
	status, _ := driver_infrastructure.BlueGreenStatusStorageType.Get(r.storageService, r.bgId+"::BlueGreenStatus")
	return status
}

func (r *SuspendConnectRouting) Apply(_ driver_infrastructure.ConnectionPlugin, _ *host_info_util.HostInfo, props *utils.RWMap[string, string],
	_ bool, pluginService driver_infrastructure.PluginService) (driver.Conn, error) {
	slog.Debug(error_util.GetMessage("BlueGreenDeployment.inProgressSuspendConnect"))
	parentCtx := pluginService.GetTelemetryContext()
	telemetryFactory := pluginService.GetTelemetryFactory()
	telemetryCtx, ctx := telemetryFactory.OpenTelemetryContext(TELEMETRY_SWITCHOVER, telemetry.NESTED, parentCtx)

	pluginService.SetTelemetryContext(ctx)
	defer func() {
		telemetryCtx.CloseContext()
		pluginService.SetTelemetryContext(parentCtx)
	}()

	bgStatus := r.getBgStatus()

	timeoutMs := property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.BG_CONNECT_TIMEOUT_MS)
	holdStartTime := time.Now()
	endTime := holdStartTime.Add(time.Millisecond * time.Duration(timeoutMs))

	for time.Now().Before(endTime) && bgStatus != nil && bgStatus.GetCurrentPhase() == driver_infrastructure.IN_PROGRESS {
		r.Delay(SLEEP_TIME_DURATION, *bgStatus, pluginService, r.bgId, r.storageService)
	}

	bgStatus = r.getBgStatus()

	if bgStatus != nil && bgStatus.GetCurrentPhase() == driver_infrastructure.IN_PROGRESS {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("BlueGreenDeployment.inProgressTryConnectLater", timeoutMs))
	}
	message := error_util.GetMessage("BlueGreenDeployment.switchoverCompleteContinueWithConnect", time.Since(holdStartTime))
	slog.Debug(message)

	return nil, error_util.NewGenericAwsWrapperError(message)
}

func NewSuspendConnectRouting(hostAndPort string, role driver_infrastructure.BlueGreenRole, bgId string, storageService driver_infrastructure.StorageService) *SuspendConnectRouting {
	return &SuspendConnectRouting{bgId, storageService, NewBaseRouting(hostAndPort, role)}
}

type SuspendUntilCorrespondingHostFoundConnectRouting struct {
	bgId           string
	storageService driver_infrastructure.StorageService
	BaseRouting
}

func (r *SuspendUntilCorrespondingHostFoundConnectRouting) getBgStatus() *driver_infrastructure.BlueGreenStatus {
	status, _ := driver_infrastructure.BlueGreenStatusStorageType.Get(r.storageService, r.bgId+"::BlueGreenStatus")
	return status
}

func (r *SuspendUntilCorrespondingHostFoundConnectRouting) Apply(
	_ driver_infrastructure.ConnectionPlugin,
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	_ bool,
	pluginService driver_infrastructure.PluginService) (driver.Conn, error) {
	slog.Debug(error_util.GetMessage("BlueGreenDeployment.waitConnectUntilCorrespondingHostFound", hostInfo.GetHost()))
	parentCtx := pluginService.GetTelemetryContext()
	telemetryFactory := pluginService.GetTelemetryFactory()
	telemetryCtx, ctx := telemetryFactory.OpenTelemetryContext(TELEMETRY_SWITCHOVER, telemetry.NESTED, parentCtx)

	pluginService.SetTelemetryContext(ctx)
	defer func() {
		telemetryCtx.CloseContext()
		pluginService.SetTelemetryContext(parentCtx)
	}()

	bgStatus := r.getBgStatus()
	var correspondingPair utils.Pair[*host_info_util.HostInfo, *host_info_util.HostInfo]
	if bgStatus != nil {
		correspondingPair = bgStatus.GetCorrespondingHosts()[hostInfo.Host]
	}

	timeoutMs := property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.BG_CONNECT_TIMEOUT_MS)
	holdStartTime := time.Now()
	endTime := holdStartTime.Add(time.Millisecond * time.Duration(timeoutMs))

	for time.Now().Before(endTime) && bgStatus != nil && bgStatus.GetCurrentPhase() != driver_infrastructure.COMPLETED &&
		correspondingPair.GetRight().IsNil() {
		r.Delay(SLEEP_TIME_DURATION, *bgStatus, pluginService, r.bgId, r.storageService)
		bgStatus = r.getBgStatus()
		if bgStatus != nil {
			correspondingPair = bgStatus.GetCorrespondingHosts()[hostInfo.Host]
		}
	}

	if bgStatus == nil || bgStatus.GetCurrentPhase() == driver_infrastructure.COMPLETED {
		message := error_util.GetMessage("BlueGreenDeployment.completedContinueWithConnect", time.Since(holdStartTime))
		slog.Debug(message)
		return nil, error_util.NewGenericAwsWrapperError(message)
	} else if time.Now().After(endTime) {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("BlueGreenDeployment.correspondingHostNotFoundTryConnectLater", hostInfo.GetHost(), timeoutMs))
	}

	message := error_util.GetMessage("BlueGreenDeployment.correspondingHostFoundContinueWithConnect", hostInfo.GetHost(), time.Since(holdStartTime))
	slog.Debug(message)

	return nil, nil
}

func NewSuspendUntilCorrespondingHostFoundConnectRouting(hostAndPort string, role driver_infrastructure.BlueGreenRole,
	bgId string, storageService driver_infrastructure.StorageService) *SuspendUntilCorrespondingHostFoundConnectRouting {
	return &SuspendUntilCorrespondingHostFoundConnectRouting{bgId, storageService, NewBaseRouting(hostAndPort, role)}
}
