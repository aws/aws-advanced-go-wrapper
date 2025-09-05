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
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

type BlueGreenProviderSupplier = func(pluginService driver_infrastructure.PluginService, props map[string]string, bgdId string) *BlueGreenStatusProvider

type BlueGreenStatusProvider struct {
	pluginService                           driver_infrastructure.PluginService
	props                                   map[string]string
	bgdId                                   string
	statusCheckIntervalMap                  map[driver_infrastructure.BlueGreenIntervalRate]int
	switchoverDuration                      time.Duration
	postStatusEndTime                       time.Time
	latestStatusPhase                       driver_infrastructure.BlueGreenPhase
	summaryStatus                           driver_infrastructure.BlueGreenStatus
	suspendNewBlueConnectionsWhenInProgress bool
	rollback                                bool
	blueDnsUpdateCompleted                  bool
	greenDnsRemoved                         bool
	greenTopologyChanged                    bool
	monitors                                []*BlueGreenStatusMonitor
	correspondingHosts                      *utils.RWMap[utils.Pair[*host_info_util.HostInfo, *host_info_util.HostInfo]]
	roleByHost                              *utils.RWMap[driver_infrastructure.BlueGreenRole]
	phaseTimeNano                           *utils.RWMap[PhaseTimeInfo]
	iamHostSuccessfulConnects               *utils.RWMap[[]string]
	hostIpAddresses                         *utils.RWMap[string]
	greenHostChangeNameTimes                *utils.RWMap[time.Time]
	interimStatuses                         []BlueGreenInterimStatus
	interimStatusHashes                     []uint64
	lastContextHash                         uint64
	allGreenHostsChangedName                atomic.Bool
	processStatusLock                       sync.Mutex
}

func NewBlueGreenStatusProvider(pluginService driver_infrastructure.PluginService, props map[string]string, bgId string) *BlueGreenStatusProvider {
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
	dialect := pluginService.GetDialect()
	if _, ok := dialect.(driver_infrastructure.BlueGreenDialect); ok {
		provider.initMonitoring()
	} else {
		slog.Warn(error_util.GetMessage("BlueGreenDeployment.unsupportedDialect", bgId, reflect.TypeOf(dialect)))
	}
	return provider
}

func (b *BlueGreenStatusProvider) ClearMonitors() {
	for _, monitor := range b.monitors {
		if monitor != nil {
			monitor.stop.Store(true)
			monitor.wg.Wait()
		}
	}
}

func (b *BlueGreenStatusProvider) initMonitoring() {
	currentHostInfo, _ := b.pluginService.GetCurrentHostInfo()
	monitoringProps := b.GetMonitoringProperties()
	b.monitors[driver_infrastructure.SOURCE.GetValue()] = NewBlueGreenStatusMonitor(
		driver_infrastructure.SOURCE,
		b.bgdId,
		currentHostInfo,
		b.pluginService,
		monitoringProps,
		b.statusCheckIntervalMap,
		b.PrepareStatus)

	b.monitors[driver_infrastructure.TARGET.GetValue()] = NewBlueGreenStatusMonitor(
		driver_infrastructure.TARGET,
		b.bgdId,
		currentHostInfo,
		b.pluginService,
		monitoringProps,
		b.statusCheckIntervalMap,
		b.PrepareStatus)
}

func (b *BlueGreenStatusProvider) GetMonitoringProperties() map[string]string {
	monitoringConnectionProps := utils.CreateMapCopy(b.props)
	for propKey, propValue := range b.props {
		if strings.HasPrefix(propKey, property_util.BG_PROPERTY_PREFIX) {
			monitoringConnectionProps[strings.TrimPrefix(propKey, property_util.BG_PROPERTY_PREFIX)] = propValue
			delete(monitoringConnectionProps, propKey)
		}
	}
	return monitoringConnectionProps
}

func (b *BlueGreenStatusProvider) PrepareStatus(role driver_infrastructure.BlueGreenRole, interimStatus BlueGreenInterimStatus) {
	b.processStatusLock.Lock()
	defer b.processStatusLock.Unlock()

	statusHash := interimStatus.GetCustomHashCode()
	contextHash := b.getContextHash()
	if b.interimStatusHashes[role.GetValue()] == statusHash && b.lastContextHash == contextHash {
		// No changes.
		return
	}

	// Some changes detected. Update summary status.
	slog.Debug(error_util.GetMessage("BlueGreenDeployment.interimStatus", b.bgdId, role, interimStatus))
	b.UpdatePhase(role, interimStatus)

	// Store interim status and corresponding hash.
	b.interimStatuses[role.GetValue()] = interimStatus
	b.interimStatusHashes[role.GetValue()] = statusHash
	b.lastContextHash = contextHash

	// Update map of IP addresses.
	for key, val := range interimStatus.startIpAddressesByHostMap {
		b.hostIpAddresses.Put(key, val)
	}

	// Update roleByHost based on provided host names.
	for hostName := range interimStatus.hostNames {
		b.roleByHost.Put(hostName, role)
	}

	b.updateCorrespondingHosts()
	err := b.UpdateSummaryStatus(role, interimStatus)
	if err != nil {
		slog.Warn(err.Error())
		return
	}

	err = b.UpdateMonitors()
	if err != nil {
		slog.Warn(err.Error())
		return
	}
	b.updateStatusCache()
	b.LogCurrentContext()

	// Log final switchover results.
	b.LogSwitchoverFinalSummary()

	b.ResetContextWhenCompleted()
}

func (b *BlueGreenStatusProvider) UpdatePhase(role driver_infrastructure.BlueGreenRole, interimStatus BlueGreenInterimStatus) {
	roleStatus := b.interimStatuses[role.GetValue()]
	latestInterimPhase := driver_infrastructure.NOT_CREATED
	if !roleStatus.IsZero() {
		latestInterimPhase = roleStatus.phase
	}

	if interimStatus.IsZero() || interimStatus.phase.IsZero() {
		return
	}

	if !latestInterimPhase.IsZero() && interimStatus.phase.GetPhase() < latestInterimPhase.GetPhase() {
		b.rollback = true
		slog.Debug(error_util.GetMessage("BlueGreenDeployment.rollback", b.bgdId))
	}

	// Do not allow status to move backward unless it is a rollback.
	shouldUpdate := false
	if b.rollback {
		shouldUpdate = interimStatus.phase.GetPhase() < b.latestStatusPhase.GetPhase()
	} else {
		shouldUpdate = interimStatus.phase.GetPhase() >= b.latestStatusPhase.GetPhase()
	}
	if shouldUpdate {
		b.latestStatusPhase = interimStatus.phase
	}
}

func (b *BlueGreenStatusProvider) updateStatusCache() {
	b.pluginService.SetBgStatus(b.summaryStatus, b.bgdId)
	b.StorePhaseTime(b.summaryStatus.GetCurrentPhase())
}

func (b *BlueGreenStatusProvider) updateCorrespondingHosts() {
	b.correspondingHosts.Clear()

	sourceInterimStatus := b.interimStatuses[driver_infrastructure.SOURCE.GetValue()]
	targetInterimStatus := b.interimStatuses[driver_infrastructure.TARGET.GetValue()]

	if len(sourceInterimStatus.startTopology) > 0 && len(targetInterimStatus.startTopology) > 0 {
		blueWriterHostInfo := b.GetWriterHost(driver_infrastructure.SOURCE)
		greenWriterHostInfo := b.GetWriterHost(driver_infrastructure.TARGET)
		sortedBlueReaderHostInfos := b.GetReaderHosts(driver_infrastructure.SOURCE)
		sortedGreenReaderHostInfos := b.GetReaderHosts(driver_infrastructure.TARGET)

		if !blueWriterHostInfo.IsNil() {
			b.correspondingHosts.Put(blueWriterHostInfo.GetHost(), utils.NewPair(blueWriterHostInfo, greenWriterHostInfo))
		}

		if len(sortedBlueReaderHostInfos) > 0 {
			// Map blue readers to green hosts.
			if len(sortedGreenReaderHostInfos) > 0 {
				for index, blueHostInfo := range sortedBlueReaderHostInfos {
					greenIndex := index % len(sortedGreenReaderHostInfos)
					b.correspondingHosts.Put(blueHostInfo.Host, utils.NewPair(blueHostInfo, sortedGreenReaderHostInfos[greenIndex]))
				}
			} else {
				// There's no green reader hosts. We have to map all blue reader hosts to the green writer.
				for _, blueHostInfo := range sortedBlueReaderHostInfos {
					b.correspondingHosts.Put(blueHostInfo.Host, utils.NewPair(blueHostInfo, greenWriterHostInfo))
				}
			}
		}
	}

	if len(sourceInterimStatus.hostNames) > 0 && len(targetInterimStatus.hostNames) > 0 {
		blueHosts := sourceInterimStatus.hostNames // Why is the writer not added previously? Missing a line?
		greenHosts := targetInterimStatus.hostNames

		blueClusterHost := utils.FilterSetFindFirst(blueHosts, func(s string) bool {
			return utils.IsWriterClusterDns(s)
		})
		greenClusterHost := utils.FilterSetFindFirst(greenHosts, func(s string) bool {
			return utils.IsWriterClusterDns(s)
		})

		if blueClusterHost != "" && greenClusterHost != "" {
			blueHost, _ := host_info_util.NewHostInfoBuilder().SetHost(blueClusterHost).Build()
			greenHost, _ := host_info_util.NewHostInfoBuilder().SetHost(greenClusterHost).Build()
			b.correspondingHosts.PutIfAbsent(blueClusterHost, utils.NewPair(blueHost, greenHost))
		}

		blueClusterReaderHost := utils.FilterSetFindFirst(blueHosts, func(s string) bool {
			return utils.IsReaderClusterDns(s)
		})
		greenClusterReaderHost := utils.FilterSetFindFirst(greenHosts, func(s string) bool {
			return utils.IsReaderClusterDns(s)
		})

		if blueClusterReaderHost != "" && greenClusterReaderHost != "" {
			blueHost, _ := host_info_util.NewHostInfoBuilder().SetHost(blueClusterReaderHost).Build()
			greenHost, _ := host_info_util.NewHostInfoBuilder().SetHost(greenClusterReaderHost).Build()
			b.correspondingHosts.PutIfAbsent(blueClusterReaderHost, utils.NewPair(blueHost, greenHost))
		}

		for blueHost := range blueHosts {
			if utils.IsRdsCustomClusterDns(blueHost) {
				customClusterName := utils.GetRdsClusterId(blueHost)
				if customClusterName != "" {
					for greenHost := range greenHosts {
						if utils.IsRdsCustomClusterDns(greenHost) && customClusterName == utils.RemoveGreenInstancePrefix(utils.GetRdsClusterId(greenHost)) {
							blueHostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost(blueHost).Build()
							greenHostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost(greenHost).Build()
							b.correspondingHosts.PutIfAbsent(blueHost, utils.NewPair(blueHostInfo, greenHostInfo))
							break
						}
					}
				}
			}
		}
	}
}

func (b *BlueGreenStatusProvider) GetWriterHost(role driver_infrastructure.BlueGreenRole) *host_info_util.HostInfo {
	interimStatus := b.interimStatuses[role.GetValue()]
	if interimStatus.IsZero() {
		return nil
	}
	return host_info_util.GetWriter(interimStatus.startTopology)
}

func (b *BlueGreenStatusProvider) GetReaderHosts(role driver_infrastructure.BlueGreenRole) []*host_info_util.HostInfo {
	interimStatus := b.interimStatuses[role.GetValue()]
	if interimStatus.IsZero() {
		return nil
	}
	return host_info_util.GetReaders(interimStatus.startTopology)
}

func (b *BlueGreenStatusProvider) UpdateSummaryStatus(role driver_infrastructure.BlueGreenRole, interimStatus BlueGreenInterimStatus) error {
	switch b.latestStatusPhase {
	case driver_infrastructure.NOT_CREATED:
		b.summaryStatus = driver_infrastructure.NewBgStatus(b.bgdId, driver_infrastructure.NOT_CREATED, nil, nil, nil, nil)
	case driver_infrastructure.CREATED:
		b.UpdateDnsFlags(role, interimStatus)
		b.summaryStatus = b.GetStatusOfCreated()
	case driver_infrastructure.PREPARATION:
		b.StartSwitchoverTimer()
		b.UpdateDnsFlags(role, interimStatus)
		b.summaryStatus = b.GetStatusOfPreparation()
	case driver_infrastructure.IN_PROGRESS:
		b.UpdateDnsFlags(role, interimStatus)
		b.summaryStatus = b.GetStatusOfInProgress()
	case driver_infrastructure.POST:
		b.UpdateDnsFlags(role, interimStatus)
		b.summaryStatus = b.GetStatusOfPost()
	case driver_infrastructure.COMPLETED:
		b.UpdateDnsFlags(role, interimStatus)
		b.summaryStatus = b.GetStatusOfCompleted()
	default:
		return error_util.NewGenericAwsWrapperError(error_util.GetMessage("BlueGreenDeployment.unknownPhase", b.bgdId, b.latestStatusPhase))
	}
	return nil
}

func (b *BlueGreenStatusProvider) UpdateMonitors() error {
	switch b.summaryStatus.GetCurrentPhase() {
	case driver_infrastructure.NOT_CREATED:
		for _, monitor := range b.monitors {
			monitor.SetIntervalRate(driver_infrastructure.BASELINE)
			monitor.collectedIpAddresses.Store(false)
			monitor.collectedTopology.Store(false)
			monitor.useIpAddress.Store(false)
		}
	case driver_infrastructure.CREATED:
		for _, monitor := range b.monitors {
			monitor.SetIntervalRate(driver_infrastructure.INCREASED)
			monitor.collectedIpAddresses.Store(true)
			monitor.collectedTopology.Store(true)
			monitor.useIpAddress.Store(false)
			if b.rollback {
				monitor.ResetCollectedData()
			}
		}
	case driver_infrastructure.PREPARATION:
	case driver_infrastructure.IN_PROGRESS:
	case driver_infrastructure.POST:
		for _, monitor := range b.monitors {
			monitor.SetIntervalRate(driver_infrastructure.HIGH)
			monitor.collectedIpAddresses.Store(false)
			monitor.collectedTopology.Store(false)
			monitor.useIpAddress.Store(true)
		}
	case driver_infrastructure.COMPLETED:
		for _, monitor := range b.monitors {
			monitor.SetIntervalRate(driver_infrastructure.BASELINE)
			monitor.collectedIpAddresses.Store(false)
			monitor.collectedTopology.Store(false)
			monitor.useIpAddress.Store(false)
			monitor.ResetCollectedData()
		}

		if !b.rollback {
			if sourceMonitor := b.monitors[driver_infrastructure.SOURCE.GetValue()]; sourceMonitor != nil {
				sourceMonitor.stop.Store(true)
			}
		}
	default:
		return error_util.NewGenericAwsWrapperError(error_util.GetMessage("BlueGreenDeployment.unknownPhase", b.bgdId, b.latestStatusPhase))
	}
	return nil
}

func (b *BlueGreenStatusProvider) UpdateDnsFlags(role driver_infrastructure.BlueGreenRole, interimStatus BlueGreenInterimStatus) {
	if role == driver_infrastructure.SOURCE && !b.blueDnsUpdateCompleted && interimStatus.allStartTopologyIpChanged {
		slog.Debug(error_util.GetMessage("BlueGreenDeployment.blueDnsCompleted", b.bgdId))
		b.blueDnsUpdateCompleted = true
		b.StoreBlueDnsUpdateTime()
	}

	if role == driver_infrastructure.TARGET && !b.greenDnsRemoved && interimStatus.allStartTopologyEndpointsRemoved {
		slog.Debug(error_util.GetMessage("BlueGreenDeployment.greenDnsRemoved", b.bgdId))
		b.greenDnsRemoved = true
		b.StoreGreenDnsRemoveTime()
	}

	if role == driver_infrastructure.TARGET && !b.greenTopologyChanged && interimStatus.allTopologyChanged {
		slog.Debug(error_util.GetMessage("BlueGreenDeployment.greenTopologyChanged", b.bgdId))
		b.greenTopologyChanged = true
		b.StoreGreenTopologyChangeTime()
	}
}

func (b *BlueGreenStatusProvider) GetStatusOfCreated() driver_infrastructure.BlueGreenStatus {
	return driver_infrastructure.NewBgStatus(
		b.bgdId,
		driver_infrastructure.CREATED,
		nil,
		nil,
		b.roleByHost,
		b.correspondingHosts,
	)
}

func (b *BlueGreenStatusProvider) GetStatusOfPreparation() driver_infrastructure.BlueGreenStatus {
	if b.IsSwitchoverTimerExpired() {
		slog.Debug(error_util.GetMessage("BlueGreenDeployment.switchoverTimeout"))
		if b.rollback {
			return b.GetStatusOfCreated()
		}
		return b.GetStatusOfCompleted()
	}
	connectRoutings := b.AddSubstituteBlueWithIpAddressConnectRouting()
	return driver_infrastructure.NewBgStatus(
		b.bgdId,
		driver_infrastructure.PREPARATION,
		connectRoutings,
		nil,
		b.roleByHost,
		b.correspondingHosts,
	)
}

func (b *BlueGreenStatusProvider) AddSubstituteBlueWithIpAddressConnectRouting() []driver_infrastructure.ConnectRouting {
	connectRoutings := make([]driver_infrastructure.ConnectRouting, 0, b.roleByHost.Size()*2)
	for host, role := range b.roleByHost.GetAllEntries() {
		hostPair, ok := b.correspondingHosts.Get(host)
		if !ok || role != driver_infrastructure.SOURCE {
			continue
		}

		blueHostInfo := hostPair.GetLeft()
		blueIp, ok := b.hostIpAddresses.Get(blueHostInfo.Host)
		blueIpHostInfo := blueHostInfo
		if ok && blueIp != "" {
			blueIpHostInfo, _ = host_info_util.NewHostInfoBuilder().CopyFrom(blueHostInfo).SetHost(blueIp).Build()
		}

		connectRoutings = append(connectRoutings, NewSubstituteConnectRouting(
			host,
			role,
			blueIpHostInfo,
			[]*host_info_util.HostInfo{blueHostInfo},
			nil,
		))

		interimStatus := b.interimStatuses[role.GetValue()]
		if interimStatus.IsZero() {
			continue
		}

		connectRoutings = append(connectRoutings, NewSubstituteConnectRouting(
			host_info_util.GetHostAndPort(host, interimStatus.port),
			role,
			blueIpHostInfo,
			[]*host_info_util.HostInfo{blueHostInfo},
			nil,
		))
	}
	return connectRoutings
}

func (b *BlueGreenStatusProvider) GetStatusOfInProgress() driver_infrastructure.BlueGreenStatus {
	if b.IsSwitchoverTimerExpired() {
		slog.Debug(error_util.GetMessage("BlueGreenDeployment.switchoverTimeout"))
		if b.rollback {
			return b.GetStatusOfCreated()
		}
		return b.GetStatusOfCompleted()
	}

	var connectRoutings []driver_infrastructure.ConnectRouting
	if b.suspendNewBlueConnectionsWhenInProgress {
		// All blue and green connect calls should be suspended.
		connectRoutings = []driver_infrastructure.ConnectRouting{NewSuspendConnectRouting("", driver_infrastructure.SOURCE, b.bgdId)}
	} else {
		// If we're not suspending new connections then, at least, we need to use IP addresses.
		connectRoutings = b.AddSubstituteBlueWithIpAddressConnectRouting()
	}

	connectRoutings = append(connectRoutings, NewSuspendConnectRouting("", driver_infrastructure.TARGET, b.bgdId))

	// All connect calls with IP address that belongs to blue or green host should be suspended.
	uniqueIpAddresses := b.getUniqueIpAddresses()
	for ipAddress := range uniqueIpAddresses {
		if b.suspendNewBlueConnectionsWhenInProgress {
			interimStatus := b.interimStatuses[driver_infrastructure.SOURCE.GetValue()]
			if interimStatusHasIpAddress(interimStatus, ipAddress) {
				connectRoutings = b.appendSuspendConnectRouting(connectRoutings, ipAddress)
				connectRoutings = b.appendSuspendConnectRouting(connectRoutings, host_info_util.GetHostAndPort(ipAddress, interimStatus.port))
				continue
			}
		}
		// Try to confirm that the ipAddress belongs to one of the green hosts
		interimStatus := b.interimStatuses[driver_infrastructure.TARGET.GetValue()]
		if interimStatusHasIpAddress(interimStatus, ipAddress) {
			connectRoutings = b.appendSuspendConnectRouting(connectRoutings, ipAddress)
			connectRoutings = b.appendSuspendConnectRouting(connectRoutings, host_info_util.GetHostAndPort(ipAddress, interimStatus.port))
		}
	}

	executeRoutings := []driver_infrastructure.ExecuteRouting{
		NewSuspendExecuteRouting("", driver_infrastructure.SOURCE, b.bgdId),
		NewSuspendExecuteRouting("", driver_infrastructure.TARGET, b.bgdId),
	}

	// All traffic through connections with IP addresses that belong to blue or green hosts should be on hold.
	for ipAddress := range uniqueIpAddresses {
		if b.suspendNewBlueConnectionsWhenInProgress {
			interimStatus := b.interimStatuses[driver_infrastructure.SOURCE.GetValue()]
			if interimStatusHasIpAddress(interimStatus, ipAddress) {
				executeRoutings = b.appendSuspendExecuteRouting(executeRoutings, ipAddress)
				executeRoutings = b.appendSuspendExecuteRouting(executeRoutings, host_info_util.GetHostAndPort(ipAddress, interimStatus.port))
				continue
			}
		}
		// Try to confirm that the ipAddress belongs to one of the green hosts
		interimStatus := b.interimStatuses[driver_infrastructure.TARGET.GetValue()]
		if interimStatusHasIpAddress(interimStatus, ipAddress) {
			executeRoutings = b.appendSuspendExecuteRouting(executeRoutings, ipAddress)
			executeRoutings = b.appendSuspendExecuteRouting(executeRoutings, host_info_util.GetHostAndPort(ipAddress, interimStatus.port))
			continue
		}
		executeRoutings = b.appendSuspendExecuteRouting(executeRoutings, ipAddress)
	}

	return driver_infrastructure.NewBgStatus(
		b.bgdId,
		driver_infrastructure.IN_PROGRESS,
		connectRoutings,
		executeRoutings,
		b.roleByHost,
		b.correspondingHosts,
	)
}

func (b *BlueGreenStatusProvider) getUniqueIpAddresses() map[string]bool {
	uniqueIpAddresses := make(map[string]bool, b.hostIpAddresses.Size())
	for _, ipAddress := range b.hostIpAddresses.GetAllEntries() {
		if _, ok := uniqueIpAddresses[ipAddress]; ipAddress != "" && !ok {
			uniqueIpAddresses[ipAddress] = true
		}
	}
	return uniqueIpAddresses
}

func (b *BlueGreenStatusProvider) appendSuspendConnectRouting(connectRoutings []driver_infrastructure.ConnectRouting, host string) []driver_infrastructure.ConnectRouting {
	return append(connectRoutings, NewSuspendConnectRouting(host, driver_infrastructure.BlueGreenRole{}, b.bgdId))
}

func (b *BlueGreenStatusProvider) appendSuspendExecuteRouting(executeRoutings []driver_infrastructure.ExecuteRouting, host string) []driver_infrastructure.ExecuteRouting {
	return append(executeRoutings, NewSuspendExecuteRouting(host, driver_infrastructure.BlueGreenRole{}, b.bgdId))
}

func (b *BlueGreenStatusProvider) GetStatusOfPost() driver_infrastructure.BlueGreenStatus {
	if b.IsSwitchoverTimerExpired() {
		slog.Debug(error_util.GetMessage("BlueGreenDeployment.switchoverTimeout"))
		if b.rollback {
			return b.GetStatusOfCreated()
		}
		return b.GetStatusOfCompleted()
	}
	return driver_infrastructure.NewBgStatus(
		b.bgdId,
		driver_infrastructure.POST,
		b.CreatePostRouting(),
		[]driver_infrastructure.ExecuteRouting{},
		b.roleByHost,
		b.correspondingHosts,
	)
}

func (b *BlueGreenStatusProvider) CreatePostRouting() (connectRoutings []driver_infrastructure.ConnectRouting) {
	if b.blueDnsUpdateCompleted && b.allGreenHostsChangedName.Load() {
		return
	}

	for host, role := range b.roleByHost.GetAllEntries() {
		if !b.blueHostInCorrespondingHosts(host, role) {
			continue
		}
		hostPair, ok := b.correspondingHosts.Get(host)
		greenHostInfo := hostPair.GetRight()
		if !ok || greenHostInfo.IsNil() {
			// A corresponding host is not found. We need to suspend this call.
			connectRoutings = append(connectRoutings, NewSuspendUntilCorrespondingHostFoundConnectRouting(
				host,
				role,
				b.bgdId,
			))
			interimStatus := b.interimStatuses[role.GetValue()]
			if !interimStatus.IsZero() {
				connectRoutings = append(connectRoutings, NewSuspendUntilCorrespondingHostFoundConnectRouting(
					host_info_util.GetHostAndPort(host, interimStatus.port),
					role,
					b.bgdId,
				))
			}
		} else {
			greenHost := greenHostInfo.Host
			greenIp, ok := b.hostIpAddresses.Get(greenHost)
			var greenHostInfoWithIp *host_info_util.HostInfo
			if ok && greenIp != "" {
				greenHostInfoWithIp, _ = host_info_util.NewHostInfoBuilder().CopyFrom(greenHostInfo).SetHost(greenIp).Build()
			} else {
				greenHostInfoWithIp = greenHostInfo
			}
			blueHostInfo := hostPair.GetLeft()
			var iamHosts []*host_info_util.HostInfo
			if b.IsAlreadySuccessfullyConnected(greenHost, host) && !blueHostInfo.IsNil() {
				// Green host has already changed its name, and it's not a new blue host (no prefixes).
				iamHosts = []*host_info_util.HostInfo{blueHostInfo}
			} else if !blueHostInfo.IsNil() {
				// Green host hasn't yet changed its name, so we need to try both possible IAM host options.
				iamHosts = []*host_info_util.HostInfo{greenHostInfo, blueHostInfo}
			} else {
				iamHosts = []*host_info_util.HostInfo{greenHostInfo}
			}
			var iamSuccessfulConnectNotify func(iamHost string) = nil
			if utils.IsRdsInstance(host) {
				iamSuccessfulConnectNotify = func(iamHost string) {
					b.RegisterIamHost(greenHost, iamHost)
				}
			}

			connectRoutings = append(connectRoutings, NewSubstituteConnectRouting(
				host,
				role,
				greenHostInfoWithIp,
				iamHosts,
				iamSuccessfulConnectNotify,
			))
			interimStatus := b.interimStatuses[role.GetValue()]
			if !interimStatus.IsZero() {
				connectRoutings = append(connectRoutings, NewSubstituteConnectRouting(
					host_info_util.GetHostAndPort(host, interimStatus.port),
					role,
					greenHostInfoWithIp,
					iamHosts,
					iamSuccessfulConnectNotify,
				))
			}
		}
	}

	if !b.greenDnsRemoved {
		// New connect calls to green endpoints should be rejected.
		connectRoutings = append(connectRoutings, NewRejectConnectRouting("", driver_infrastructure.TARGET))
	}
	return connectRoutings
}

func (b *BlueGreenStatusProvider) GetStatusOfCompleted() driver_infrastructure.BlueGreenStatus {
	if b.IsSwitchoverTimerExpired() {
		slog.Debug(error_util.GetMessage("BlueGreenDeployment.switchoverTimeout"))
		if b.rollback {
			return b.GetStatusOfCreated()
		}
		return driver_infrastructure.NewBgStatus(
			b.bgdId,
			driver_infrastructure.COMPLETED,
			[]driver_infrastructure.ConnectRouting{},
			[]driver_infrastructure.ExecuteRouting{},
			b.roleByHost,
			b.correspondingHosts,
		)
	}
	// BGD reports that it's completed but DNS hasn't yet updated completely.
	// Pretend that status isn't (yet) completed.
	if !b.blueDnsUpdateCompleted || !b.greenDnsRemoved {
		return b.GetStatusOfPost()
	}
	return driver_infrastructure.NewBgStatus(
		b.bgdId,
		driver_infrastructure.COMPLETED,
		[]driver_infrastructure.ConnectRouting{},
		[]driver_infrastructure.ExecuteRouting{},
		b.roleByHost,
		utils.NewRWMap[utils.Pair[*host_info_util.HostInfo, *host_info_util.HostInfo]](),
	)
}

func (b *BlueGreenStatusProvider) RegisterIamHost(connectHost string, iamHost string) {
	differentHostNames := connectHost != "" && connectHost != iamHost
	if differentHostNames && !b.IsAlreadySuccessfullyConnected(connectHost, iamHost) {
		b.greenHostChangeNameTimes.Put(connectHost, time.Now())
		slog.Debug(error_util.GetMessage("BlueGreenDeployment.greenHostChangedName", connectHost, iamHost))
	}

	if successfulConnects, ok := b.iamHostSuccessfulConnects.Get(connectHost); ok {
		b.iamHostSuccessfulConnects.Put(connectHost, append(successfulConnects, iamHost))
	} else {
		b.iamHostSuccessfulConnects.Put(connectHost, []string{iamHost})
	}

	if differentHostNames {
		// Check all IAM hosts have changed their names
		allHostChangedNames := true
		for key, val := range b.iamHostSuccessfulConnects.GetAllEntries() {
			if len(val) > 0 && utils.FilterSliceFindFirst(val, func(s string) bool {
				return s != key
			}) == "" {
				allHostChangedNames = false
				break
			}
		}
		if allHostChangedNames && !b.allGreenHostsChangedName.Load() {
			slog.Debug(error_util.GetMessage("BlueGreenDeployment.allGreenHostChangedName"))
			b.allGreenHostsChangedName.Store(true)
			b.StoreGreenHostChangeNameTime()
		}
	}
}

func (b *BlueGreenStatusProvider) IsAlreadySuccessfullyConnected(connectHost string, iamHost string) bool {
	successfulConnects, ok := b.iamHostSuccessfulConnects.Get(connectHost)
	return ok && slices.Contains(successfulConnects, iamHost)
}

func (b *BlueGreenStatusProvider) getContextHash() uint64 {
	return getValueHash(getValueHash(1, strconv.FormatBool(b.allGreenHostsChangedName.Load())), strconv.Itoa(b.iamHostSuccessfulConnects.Size()))
}

func (b *BlueGreenStatusProvider) StorePhaseTime(phase driver_infrastructure.BlueGreenPhase) {
	if phase.IsZero() {
		return
	}
	b.PutIfAbsentPhaseTime(phase.GetName(), phase)
}

func (b *BlueGreenStatusProvider) PutIfAbsentPhaseTime(key string, phase driver_infrastructure.BlueGreenPhase) {
	if b.rollback {
		key += " (rollback)"
	}
	b.phaseTimeNano.PutIfAbsent(key, PhaseTimeInfo{
		time.Now(),
		phase,
	})
}

func (b *BlueGreenStatusProvider) StoreBlueDnsUpdateTime() {
	b.PutIfAbsentPhaseTime("Blue DNS updated", driver_infrastructure.BlueGreenPhase{})
}

func (b *BlueGreenStatusProvider) StoreGreenDnsRemoveTime() {
	b.PutIfAbsentPhaseTime("Green DNS removed", driver_infrastructure.BlueGreenPhase{})
}

func (b *BlueGreenStatusProvider) StoreGreenHostChangeNameTime() {
	b.PutIfAbsentPhaseTime("Green host certificates changed", driver_infrastructure.BlueGreenPhase{})
}

func (b *BlueGreenStatusProvider) StoreGreenTopologyChangeTime() {
	b.PutIfAbsentPhaseTime("Green topology changed", driver_infrastructure.BlueGreenPhase{})
}

func (b *BlueGreenStatusProvider) StartSwitchoverTimer() {
	if b.postStatusEndTime.Equal(time.Time{}) {
		b.postStatusEndTime = time.Now().Add(b.switchoverDuration)
	}
}

func (b *BlueGreenStatusProvider) IsSwitchoverTimerExpired() bool {
	return !b.postStatusEndTime.Equal(time.Time{}) && b.postStatusEndTime.Before(time.Now())
}

func (b *BlueGreenStatusProvider) ResetContextWhenCompleted() {
	switchoverCompleted := (!b.rollback && b.summaryStatus.GetCurrentPhase() == driver_infrastructure.COMPLETED) ||
		(b.rollback && b.summaryStatus.GetCurrentPhase() == driver_infrastructure.CREATED)

	hasActiveSwitchoverPhase := utils.FilterMapFindFirstValue(b.phaseTimeNano.GetAllEntries(), func(p PhaseTimeInfo) bool {
		return !p.Phase.IsZero() && p.Phase.IsActiveSwitchoverOrCompleted()
	}) != PhaseTimeInfo{}

	if switchoverCompleted && hasActiveSwitchoverPhase {
		slog.Debug(error_util.GetMessage("BlueGreenDeployment.resetContext"))
		b.rollback, b.greenDnsRemoved, b.greenTopologyChanged = false, false, false
		b.allGreenHostsChangedName.Store(false)
		b.postStatusEndTime = time.Time{}
		b.lastContextHash = 0
		b.interimStatusHashes = []uint64{0, 0}
		b.interimStatuses = []BlueGreenInterimStatus{{}, {}}
		b.latestStatusPhase = driver_infrastructure.NOT_CREATED
		b.summaryStatus = driver_infrastructure.BlueGreenStatus{} // double check nil checks etc
		b.phaseTimeNano.Clear()
		b.hostIpAddresses.Clear()
		b.correspondingHosts.Clear()
		b.roleByHost.Clear()
		b.iamHostSuccessfulConnects.Clear()
		b.greenHostChangeNameTimes.Clear()

		if !b.rollback {
			if targetMonitor := b.monitors[driver_infrastructure.TARGET.GetValue()]; targetMonitor != nil {
				targetMonitor.stop.Store(true)
			}
		}
	}
}

func (b *BlueGreenStatusProvider) LogSwitchoverFinalSummary() {
	switchoverCompleted := (!b.rollback && b.summaryStatus.GetCurrentPhase() == driver_infrastructure.COMPLETED) ||
		(b.rollback && b.summaryStatus.GetCurrentPhase() == driver_infrastructure.CREATED)

	hasActiveSwitchoverPhase := false
	for _, phaseTime := range b.phaseTimeNano.GetAllEntries() {
		if !phaseTime.Phase.IsZero() && phaseTime.Phase.IsActiveSwitchoverOrCompleted() {
			hasActiveSwitchoverPhase = true
			break
		}
	}

	if !switchoverCompleted || !hasActiveSwitchoverPhase {
		return
	}

	var timeZeroPhase driver_infrastructure.BlueGreenPhase
	var timeZeroKey string
	if b.rollback {
		timeZeroPhase = driver_infrastructure.PREPARATION
		timeZeroKey = timeZeroPhase.GetName() + " (rollback)"
	} else {
		timeZeroPhase = driver_infrastructure.IN_PROGRESS
		timeZeroKey = timeZeroPhase.GetName()
	}

	timeZero, hasTimeZero := b.phaseTimeNano.Get(timeZeroKey)
	divider := "----------------------------------------------------------------------------------\n"

	// Create sorted slice of phase time entries
	type phaseEntry struct {
		key       string
		phaseTime PhaseTimeInfo
	}
	var entries []phaseEntry
	for key, phaseTime := range b.phaseTimeNano.GetAllEntries() {
		entries = append(entries, phaseEntry{key: key, phaseTime: phaseTime})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].phaseTime.Timestamp.Before(entries[j].phaseTime.Timestamp)
	})

	var logMessage strings.Builder
	logMessage.WriteString(fmt.Sprintf("[bgdId: '%s']", b.bgdId))
	logMessage.WriteString("\n")
	logMessage.WriteString(divider)
	logMessage.WriteString(fmt.Sprintf("%-28s %21s %31s\n", "timestamp", "time offset (ms)", "event"))
	logMessage.WriteString(divider)

	for _, entry := range entries {
		timestampStr := entry.phaseTime.Timestamp.Format("2006-01-02T15:04:05.000Z")
		var offsetStr string
		if hasTimeZero {
			offsetMs := entry.phaseTime.Timestamp.Sub(timeZero.Timestamp).Milliseconds()
			offsetStr = fmt.Sprintf("%d ms", offsetMs)
		}
		logMessage.WriteString(fmt.Sprintf("%28s %18s %31s\n", timestampStr, offsetStr, entry.key))
	}
	logMessage.WriteString(divider)

	slog.Info(logMessage.String())
}

func (b *BlueGreenStatusProvider) LogCurrentContext() {
	if !slog.Default().Enabled(context.TODO(), slog.LevelDebug) {
		// We can skip this log message if debug level is in effect
		// and more detailed message is going to be printed few lines below.
		var currentPhaseStr string
		if b.summaryStatus.IsZero() || b.summaryStatus.GetCurrentPhase().IsZero() {
			currentPhaseStr = "<null>"
		} else {
			currentPhaseStr = b.summaryStatus.GetCurrentPhase().GetName()
		}
		slog.Info(fmt.Sprintf("[bgdId: '%s'] BG status: %s", b.bgdId, currentPhaseStr))
	}

	var summaryStatusStr string
	if b.summaryStatus.IsZero() {
		summaryStatusStr = "<null>"
	} else {
		summaryStatusStr = b.summaryStatus.String()
	}
	slog.Debug(fmt.Sprintf("[bgdId: '%s'] Summary status:\n%s", b.bgdId, summaryStatusStr))

	var correspondingHostsBuilder strings.Builder
	correspondingHostsBuilder.WriteString("Corresponding hosts:\n")
	for key, value := range b.correspondingHosts.GetAllEntries() {
		var rightHostStr string
		if value.GetRight().IsNil() {
			rightHostStr = "<null>"
		} else {
			rightHostStr = value.GetRight().GetHostAndPort()
		}
		correspondingHostsBuilder.WriteString(fmt.Sprintf("   %s -> %s\n", key, rightHostStr))
	}
	slog.Debug(correspondingHostsBuilder.String())

	var phaseTimesBuilder strings.Builder
	phaseTimesBuilder.WriteString("Phase times:\n")
	for key, value := range b.phaseTimeNano.GetAllEntries() {
		phaseTimesBuilder.WriteString(fmt.Sprintf("   %s -> %s\n", key, value.Timestamp.Format("2006-01-02T15:04:05.000Z")))
	}
	slog.Debug(phaseTimesBuilder.String())

	var greenHostChangeTimesBuilder strings.Builder
	greenHostChangeTimesBuilder.WriteString("Green host certificate change times:\n")
	for key, value := range b.greenHostChangeNameTimes.GetAllEntries() {
		greenHostChangeTimesBuilder.WriteString(fmt.Sprintf("   %s -> %s\n", key, value.Format("2006-01-02T15:04:05.000Z")))
	}
	slog.Debug(greenHostChangeTimesBuilder.String())

	var latestStatusPhaseName string
	if b.latestStatusPhase.IsZero() {
		latestStatusPhaseName = "<null>"
	} else {
		latestStatusPhaseName = b.latestStatusPhase.GetName()
	}

	slog.Debug(fmt.Sprintf("\n"+
		"   latestStatusPhase: %s\n"+
		"   blueDnsUpdateCompleted: %t\n"+
		"   greenDnsRemoved: %t\n"+
		"   greenHostChangedName: %t\n"+
		"   greenTopologyChanged: %t",
		latestStatusPhaseName,
		b.blueDnsUpdateCompleted,
		b.greenDnsRemoved,
		b.allGreenHostsChangedName.Load(),
		b.greenTopologyChanged))
}

func (b *BlueGreenStatusProvider) isZero() bool {
	return b == nil || (b.pluginService == nil && b.props == nil && b.bgdId == "")
}

func (b *BlueGreenStatusProvider) blueHostInCorrespondingHosts(host string, role driver_infrastructure.BlueGreenRole) bool {
	return role == driver_infrastructure.SOURCE && utils.FilterSetFindFirst(b.correspondingHosts.GetAllEntries(), func(s string) bool {
		return s == host
	}) != ""
}

func interimStatusHasIpAddress(interimStatus BlueGreenInterimStatus, ipAddress string) bool {
	firstMatchToIpAddress := utils.FilterMapFindFirstValue(interimStatus.startIpAddressesByHostMap, func(s string) bool {
		return s != "" && s == ipAddress
	})
	return !interimStatus.IsZero() && firstMatchToIpAddress != ""
}
