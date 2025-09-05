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
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

type TestBlueGreenStatusMonitor struct {
	*BlueGreenStatusMonitor
}

func NewTestBlueGreenStatusMonitor(blueGreenRole driver_infrastructure.BlueGreenRole, bgdId string, hostInfo *host_info_util.HostInfo,
	pluginService driver_infrastructure.PluginService, monitoringProps map[string]string, statusCheckIntervalMap map[driver_infrastructure.BlueGreenIntervalRate]int,
	onBlueGreenStatusChangeFunc func(role driver_infrastructure.BlueGreenRole, interimStatus BlueGreenInterimStatus)) *TestBlueGreenStatusMonitor {
	dialect, _ := pluginService.GetDialect().(driver_infrastructure.BlueGreenDialect)
	monitor := BlueGreenStatusMonitor{
		role:                        blueGreenRole,
		bgId:                        bgdId,
		initialHostInfo:             hostInfo,
		pluginService:               pluginService,
		props:                       monitoringProps,
		statusCheckIntervalMap:      statusCheckIntervalMap,
		onBlueGreenStatusChangeFunc: onBlueGreenStatusChangeFunc,
		blueGreenDialect:            dialect,
		currentPhase:                driver_infrastructure.NOT_CREATED,
		version:                     "1.0",
		port:                        -1,
		startIpAddressesByHostMap:   utils.NewRWMap[string](),
		currentIpAddressesByHostMap: utils.NewRWMap[string](),
		hostNames:                   utils.NewRWMap[bool](),
		startTopology:               []*host_info_util.HostInfo{},
	}
	monitor.stop.Store(true)
	monitor.panicMode.Store(true)
	monitor.intervalRate.Store(int32(driver_infrastructure.BASELINE))
	return &TestBlueGreenStatusMonitor{&monitor}
}

func (t *TestBlueGreenStatusMonitor) GetPanicMode() bool {
	return t.panicMode.Load()
}

func (t *TestBlueGreenStatusMonitor) SetPanicMode(val bool) {
	if t.panicMode.Load() != val {
		t.panicMode.Store(val)
	}
}

func (t *TestBlueGreenStatusMonitor) SetCollectedTopology(val bool) {
	if t.collectedTopology.Load() != val {
		t.collectedTopology.Store(val)
	}
}

func (t *TestBlueGreenStatusMonitor) SetCollectedIpAddresses(val bool) {
	if t.collectedIpAddresses.Load() != val {
		t.collectedIpAddresses.Store(val)
	}
}

func (t *TestBlueGreenStatusMonitor) SetConnectionHostInfoCorrect(val bool) {
	if t.connectionHostInfoCorrect.Load() != val {
		t.connectionHostInfoCorrect.Store(val)
	}
}

func (t *TestBlueGreenStatusMonitor) SetUseIpAddress(val bool) {
	if t.useIpAddress.Load() != val {
		t.useIpAddress.Store(val)
	}
}

func (t *TestBlueGreenStatusMonitor) SetStop(val bool) {
	if t.stop.Load() != val {
		t.stop.Store(val)
	}
}

func (t *TestBlueGreenStatusMonitor) GetConnection() *driver.Conn {
	return t.connection.Load()
}

func (t *TestBlueGreenStatusMonitor) SetConnection(val *driver.Conn) {
	if t.connection.Load() != val {
		t.connection.Store(val)
	}
}

func (t *TestBlueGreenStatusMonitor) GetHostListProvider() driver_infrastructure.HostListProvider {
	return t.hostListProvider
}

func (t *TestBlueGreenStatusMonitor) SetHostListProvider(val driver_infrastructure.HostListProvider) {
	t.hostListProvider = val
}

func (t *TestBlueGreenStatusMonitor) SetAllStartTopologyIpChanged(val bool) {
	t.allStartTopologyIpChanged = val
}

func (t *TestBlueGreenStatusMonitor) GetAllStartTopologyIpChanged() bool {
	return t.allStartTopologyIpChanged
}

func (t *TestBlueGreenStatusMonitor) SetAllStartTopologyEndpointsRemoved(val bool) {
	t.allStartTopologyEndpointsRemoved = val
}

func (t *TestBlueGreenStatusMonitor) GetAllStartTopologyEndpointsRemoved() bool {
	return t.allStartTopologyEndpointsRemoved
}

func (t *TestBlueGreenStatusMonitor) SetAllTopologyChanged(val bool) {
	t.allTopologyChanged = val
}

func (t *TestBlueGreenStatusMonitor) GetAllTopologyChanged() bool {
	return t.allTopologyChanged
}

func (t *TestBlueGreenStatusMonitor) GetStartIpAddressesByHostMap() *utils.RWMap[string] {
	return t.startIpAddressesByHostMap
}

func (t *TestBlueGreenStatusMonitor) GetCurrentIpAddressesByHostMap() *utils.RWMap[string] {
	return t.currentIpAddressesByHostMap
}

func (t *TestBlueGreenStatusMonitor) GetHostNames() *utils.RWMap[bool] {
	return t.hostNames
}

func (t *TestBlueGreenStatusMonitor) GetCurrentPhase() driver_infrastructure.BlueGreenPhase {
	return t.currentPhase
}

func (t *TestBlueGreenStatusMonitor) SetStartTopology(val []*host_info_util.HostInfo) {
	t.startTopology = val
}

func (t *TestBlueGreenStatusMonitor) SetCurrentTopology(val *[]*host_info_util.HostInfo) {
	if t.currentTopology.Load() != val {
		t.currentTopology.Store(val)
	}
}

func (t *TestBlueGreenStatusMonitor) SetConnectedIpAddress(val string) {
	if t.connectedIpAddress.Load() != val {
		t.connectedIpAddress.Store(val)
	}
}

func (t *TestBlueGreenStatusMonitor) SetConnectionHostInfo(val *host_info_util.HostInfo) {
	if t.connectionHostInfo.Load() != val {
		t.connectionHostInfo.Store(val)
	}
}

type TestBlueGreenStatusProvider struct {
	*BlueGreenStatusProvider
}

func NewTestBlueGreenStatusProvider(pluginService driver_infrastructure.PluginService, props map[string]string, bgId string) *TestBlueGreenStatusProvider {
	statusCheckIntervalMap := map[driver_infrastructure.BlueGreenIntervalRate]int{
		driver_infrastructure.BASELINE:  property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.BG_INTERVAL_BASELINE_MS),
		driver_infrastructure.INCREASED: property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.BG_INTERVAL_INCREASED_MS),
		driver_infrastructure.HIGH:      property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.BG_INTERVAL_HIGH_MS),
	}

	switchoverTimeMs := property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.BG_SWITCHOVER_TIMEOUT_MS)

	provider := &BlueGreenStatusProvider{
		pluginService:                           pluginService,
		props:                                   props,
		bgdId:                                   bgId,
		statusCheckIntervalMap:                  statusCheckIntervalMap,
		switchoverDuration:                      time.Millisecond * time.Duration(switchoverTimeMs),
		latestStatusPhase:                       driver_infrastructure.NOT_CREATED,
		monitors:                                []*BlueGreenStatusMonitor{nil, nil},
		correspondingHosts:                      utils.NewRWMap[utils.Pair[*host_info_util.HostInfo, *host_info_util.HostInfo]](),
		iamHostSuccessfulConnects:               utils.NewRWMap[[]string](),
		hostIpAddresses:                         utils.NewRWMap[string](),
		greenHostChangeNameTimes:                utils.NewRWMap[time.Time](),
		roleByHost:                              utils.NewRWMap[driver_infrastructure.BlueGreenRole](),
		phaseTimeNano:                           utils.NewRWMap[PhaseTimeInfo](),
		interimStatuses:                         []BlueGreenInterimStatus{{}, {}},
		interimStatusHashes:                     []uint64{0, 0},
		lastContextHash:                         0,
		suspendNewBlueConnectionsWhenInProgress: property_util.GetVerifiedWrapperPropertyValue[bool](props, property_util.BG_SUSPEND_NEW_BLUE_CONNECTIONS),
	}

	provider.allGreenHostsChangedName.Store(false)
	return &TestBlueGreenStatusProvider{provider}
}

func (t *TestBlueGreenStatusProvider) GetAllGreenHostsChangedName() bool {
	return t.allGreenHostsChangedName.Load()
}

func (t *TestBlueGreenStatusProvider) SetAllGreenHostsChangedName(val bool) {
	if t.allGreenHostsChangedName.Load() != val {
		t.allGreenHostsChangedName.Store(val)
	}
}

func (t *TestBlueGreenStatusProvider) GetLatestStatusPhase() driver_infrastructure.BlueGreenPhase {
	return t.latestStatusPhase
}

func (t *TestBlueGreenStatusProvider) GetCorrespondingHosts() *utils.RWMap[utils.Pair[*host_info_util.HostInfo, *host_info_util.HostInfo]] {
	return t.correspondingHosts
}

func (t *TestBlueGreenStatusProvider) GetRoleByHost() *utils.RWMap[driver_infrastructure.BlueGreenRole] {
	return t.roleByHost
}

func (t *TestBlueGreenStatusProvider) GetPhaseTimeNano() *utils.RWMap[PhaseTimeInfo] {
	return t.phaseTimeNano
}

func (t *TestBlueGreenStatusProvider) GetHostIpAddresses() *utils.RWMap[string] {
	return t.hostIpAddresses
}

func (t *TestBlueGreenStatusProvider) GetInterimStatuses() []BlueGreenInterimStatus {
	return t.interimStatuses
}

func (t *TestBlueGreenStatusProvider) GetRollback() bool {
	return t.rollback
}

func (t *TestBlueGreenStatusProvider) SetRollback(val bool) {
	t.rollback = val
}

func (t *TestBlueGreenStatusProvider) SetGreenTopologyChanged(val bool) {
	t.greenTopologyChanged = val
}

func (t *TestBlueGreenStatusProvider) GetGreenTopologyChanged() bool {
	return t.greenTopologyChanged
}

func (t *TestBlueGreenStatusProvider) SetGreenDnsRemoved(val bool) {
	t.greenDnsRemoved = val
}

func (t *TestBlueGreenStatusProvider) GetGreenDnsRemoved() bool {
	return t.greenDnsRemoved
}

func (t *TestBlueGreenStatusProvider) SetBlueDnsUpdateCompleted(val bool) {
	t.blueDnsUpdateCompleted = val
}

func (t *TestBlueGreenStatusProvider) GetBlueDnsUpdateCompleted() bool {
	return t.blueDnsUpdateCompleted
}

func (t *TestBlueGreenStatusProvider) SetSummaryStatus(val driver_infrastructure.BlueGreenStatus) {
	t.summaryStatus = val
}

func (t *TestBlueGreenStatusProvider) SetPostStatusEndTime(val time.Time) {
	t.postStatusEndTime = val
}

func (t *TestBlueGreenStatusProvider) GetPostStatusEndTime() time.Time {
	return t.postStatusEndTime
}

func NewTestBlueGreenInterimStatus(phase driver_infrastructure.BlueGreenPhase, startTopology []*host_info_util.HostInfo,
	startIpAddressesByHostMap map[string]string, ipChanged bool, endpointsRemoved bool, allChanged bool) BlueGreenInterimStatus {
	return BlueGreenInterimStatus{
		phase:                            phase,
		version:                          "1.0",
		port:                             1234,
		startTopology:                    startTopology,
		startIpAddressesByHostMap:        startIpAddressesByHostMap,
		allStartTopologyIpChanged:        ipChanged,
		allStartTopologyEndpointsRemoved: endpointsRemoved,
		allTopologyChanged:               allChanged,
	}
}

func NewTestStatusInfo() StatusInfo {
	return StatusInfo{
		version: "1.0",
	}
}
