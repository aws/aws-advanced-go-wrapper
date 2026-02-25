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
	"database/sql/driver"
	"fmt"
	"log/slog"
	"net"
	"slices"
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

const BG_CLUSTER_ID = "941d00a8-8238-4f7d-bf59-771bff783a8e"
const DEFAULT_CHECK_INTERVAL = 5 * 60_000 // 5 minutes
const latestKnownVersion = "1.0"

var knownVersions = []string{latestKnownVersion}

type BlueGreenStatusMonitor struct {
	role                             driver_infrastructure.BlueGreenRole
	bgId                             string
	initialHostInfo                  *host_info_util.HostInfo
	hostListProvider                 driver_infrastructure.HostListProvider
	pluginService                    driver_infrastructure.PluginService
	blueGreenDialect                 driver_infrastructure.BlueGreenDialect
	props                            *utils.RWMap[string, string]
	statusCheckIntervalMap           map[driver_infrastructure.BlueGreenIntervalRate]int
	onBlueGreenStatusChangeFunc      func(role driver_infrastructure.BlueGreenRole, interimStatus BlueGreenInterimStatus)
	currentPhase                     driver_infrastructure.BlueGreenPhase
	version                          string
	port                             int
	allStartTopologyIpChanged        bool
	allStartTopologyEndpointsRemoved bool
	allTopologyChanged               bool
	startIpAddressesByHostMap        *utils.RWMap[string, string]
	currentIpAddressesByHostMap      *utils.RWMap[string, string]
	hostNames                        *utils.RWMap[string, bool]
	startTopology                    []*host_info_util.HostInfo
	currentTopology                  atomic.Pointer[[]*host_info_util.HostInfo]
	connection                       atomic.Pointer[driver.Conn]
	connectionHostInfo               atomic.Pointer[host_info_util.HostInfo]
	connectedIpAddress               atomic.Value
	intervalRate                     atomic.Int32
	collectedIpAddresses             atomic.Bool
	collectedTopology                atomic.Bool
	connectionHostInfoCorrect        atomic.Bool
	useIpAddress                     atomic.Bool
	panicMode                        atomic.Bool
	stop                             atomic.Bool
	wg                               sync.WaitGroup
}

func NewBlueGreenStatusMonitor(blueGreenRole driver_infrastructure.BlueGreenRole, bgdId string, hostInfo *host_info_util.HostInfo,
	pluginService driver_infrastructure.PluginService, monitoringProps *utils.RWMap[string, string], statusCheckIntervalMap map[driver_infrastructure.BlueGreenIntervalRate]int,
	onBlueGreenStatusChangeFunc func(role driver_infrastructure.BlueGreenRole, interimStatus BlueGreenInterimStatus)) *BlueGreenStatusMonitor {
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
		startIpAddressesByHostMap:   utils.NewRWMap[string, string](),
		currentIpAddressesByHostMap: utils.NewRWMap[string, string](),
		hostNames:                   utils.NewRWMap[string, bool](),
		startTopology:               []*host_info_util.HostInfo{},
	}
	monitor.stop.Store(false)
	monitor.panicMode.Store(true)
	monitor.intervalRate.Store(int32(driver_infrastructure.BASELINE))

	go monitor.runMonitoringLoop()
	return &monitor
}

func (b *BlueGreenStatusMonitor) runMonitoringLoop() {
	b.wg.Add(1)
	defer func() {
		b.CloseConnection()
		if b.hostListProvider != nil {
			b.hostListProvider.StopMonitor()
			canReleaseResources, ok := b.hostListProvider.(driver_infrastructure.CanReleaseResources)
			if ok {
				canReleaseResources.ReleaseResources()
			}
		}
		b.hostListProvider = nil
		slog.Debug(error_util.GetMessage("BlueGreenDeployment.monitoringLoopCompleted", b.role))
		b.wg.Done()
	}()

	for !b.stop.Load() {
		var oldPhase = b.currentPhase

		b.OpenConnection()
		b.CollectStatus()
		_ = b.CollectTopology()
		b.CollectHostIpAddresses()
		b.UpdateIpAddressFlags()

		if !b.currentPhase.IsZero() && !b.currentPhase.Equals(oldPhase) {
			slog.Warn(error_util.GetMessage("BlueGreenDeployment.statusChanged", b.role, b.currentPhase))
		}

		if !b.stop.Load() && b.onBlueGreenStatusChangeFunc != nil {
			b.onBlueGreenStatusChangeFunc(
				b.role,
				BlueGreenInterimStatus{
					b.currentPhase,
					b.version,
					b.port,
					b.startTopology,
					b.GetCurrentTopology(),
					b.startIpAddressesByHostMap.GetAllEntries(),
					b.currentIpAddressesByHostMap.GetAllEntries(),
					b.hostNames.GetAllEntries(),
					b.allStartTopologyIpChanged,
					b.allStartTopologyEndpointsRemoved,
					b.allTopologyChanged,
				},
			)
		}

		b.Delay()
	}
}

func (b *BlueGreenStatusMonitor) Delay() {
	currentPanic := b.panicMode.Load()
	currentBgIntervalRate := b.GetIntervalRate()
	var delayMs int
	var ok bool
	if currentPanic {
		delayMs, ok = b.statusCheckIntervalMap[driver_infrastructure.HIGH]
	} else {
		delayMs, ok = b.statusCheckIntervalMap[currentBgIntervalRate]
	}
	if !ok || delayMs == 0 {
		delayMs = DEFAULT_CHECK_INTERVAL
	}

	endTime := time.Now().Add(time.Millisecond * time.Duration(delayMs))
	minDelayMs := min(delayMs, 50)

	for b.GetIntervalRate() == currentBgIntervalRate && time.Now().Before(endTime) && !b.stop.Load() && b.panicMode.Load() == currentPanic {
		time.Sleep(time.Duration(minDelayMs) * time.Millisecond)
	}
}

func (b *BlueGreenStatusMonitor) getBlueGreenStatus(conn driver.Conn) []driver_infrastructure.BlueGreenResult {
	query := b.blueGreenDialect.GetBlueGreenStatusQuery()
	parser := b.blueGreenDialect.GetRowParser()
	queryerCtx, ok := conn.(driver.QueryerContext)
	if !ok {
		// Unable to query, conn does not implement QueryerContext.
		slog.Warn(error_util.GetMessage("Conn.doesNotImplementRequiredInterface", "driver.QueryerContext"))
		return nil
	}

	rows, err := queryerCtx.QueryContext(context.Background(), query, nil)
	if err != nil {
		// Query failed.
		slog.Warn(error_util.GetMessage("BlueGreenDeployment.errorQueryingStatusTable", err))
		return nil
	}
	if rows != nil {
		defer rows.Close()
	}

	var statuses []driver_infrastructure.BlueGreenResult
	row := make([]driver.Value, len(rows.Columns()))
	for rows.Next(row) == nil {
		if len(row) > 4 {
			version, ok1 := parser.ParseString(row[0])
			endpoint, ok2 := parser.ParseString(row[1])
			portAsFloat, ok3 := parser.ParseInt64(row[2])
			role, ok4 := parser.ParseString(row[3])
			status, ok5 := parser.ParseString(row[4])

			if !ok1 || !ok2 || !ok3 || !ok4 || !ok5 {
				continue
			}
			statuses = append(statuses, driver_infrastructure.BlueGreenResult{
				Version:  version,
				Endpoint: endpoint,
				Port:     int(portAsFloat),
				Role:     role,
				Status:   status,
			})
		}
	}

	return statuses
}

func (b *BlueGreenStatusMonitor) SetIntervalRate(intervalRate driver_infrastructure.BlueGreenIntervalRate) {
	b.intervalRate.Store(int32(intervalRate))
}

func (b *BlueGreenStatusMonitor) GetIntervalRate() driver_infrastructure.BlueGreenIntervalRate {
	return mapToBlueGreenIntervalRate(b.intervalRate.Load())
}

func mapToBlueGreenIntervalRate(value int32) driver_infrastructure.BlueGreenIntervalRate {
	switch value {
	case int32(driver_infrastructure.BASELINE):
		return driver_infrastructure.BASELINE
	case int32(driver_infrastructure.INCREASED):
		return driver_infrastructure.INCREASED
	case int32(driver_infrastructure.HIGH):
		return driver_infrastructure.HIGH
	default:
		return driver_infrastructure.INVALID
	}
}

func (b *BlueGreenStatusMonitor) UpdateIpAddressFlags() {
	if b.collectedTopology.Load() {
		b.allStartTopologyIpChanged = false
		b.allStartTopologyEndpointsRemoved = false
		b.allTopologyChanged = false
		return
	}

	if !b.collectedIpAddresses.Load() {
		haveAllChanged := len(b.startTopology) > 0
		for _, hostInfo := range b.startTopology {
			host := hostInfo.Host
			startIp, startIpExists := b.startIpAddressesByHostMap.Get(host)
			currentIp, currentIpExists := b.currentIpAddressesByHostMap.Get(host)
			if !startIpExists || !currentIpExists || startIp == currentIp {
				// If any of the hosts in startTopology have not changed - return false.
				haveAllChanged = false
				break
			}
		}
		b.allStartTopologyIpChanged = haveAllChanged
	}

	haveAllChanged := len(b.startTopology) > 0
	for _, hostInfo := range b.startTopology {
		host := hostInfo.Host
		startIp, startIpExists := b.startIpAddressesByHostMap.Get(host)
		currentIp, currentIpExists := b.currentIpAddressesByHostMap.Get(host)
		if !startIpExists || startIp == "" || (currentIpExists && currentIp != "") {
			// If any of the hosts in startTopology still has an IP address - return false.
			haveAllChanged = false
			break
		}
	}
	b.allStartTopologyEndpointsRemoved = haveAllChanged

	if !b.collectedTopology.Load() {
		currentTopologyCopy := b.GetCurrentTopology()
		b.allTopologyChanged = len(currentTopologyCopy) > 0 && len(b.startTopology) > 0 && host_info_util.HaveNoHostsInCommon(currentTopologyCopy, b.startTopology)
	}
}

func (b *BlueGreenStatusMonitor) CollectHostIpAddresses() {
	b.currentIpAddressesByHostMap.Clear()

	if b.hostNames.Size() == 0 {
		return
	}

	for host := range b.hostNames.GetAllEntries() {
		b.currentIpAddressesByHostMap.PutIfAbsent(host, b.GetIpAddress(host))
	}

	if b.collectedIpAddresses.Load() {
		b.startIpAddressesByHostMap.ReplaceCacheWithCopy(b.currentIpAddressesByHostMap)
	}
}

func (b *BlueGreenStatusMonitor) CollectTopology() error {
	if b.hostListProvider == nil {
		return nil
	}

	conn := b.connection.Load()
	if conn == nil || b.pluginService.GetTargetDriverDialect().IsClosed(*conn) {
		return nil
	}
	hosts, err := b.hostListProvider.ForceRefresh()
	if err != nil {
		slog.Warn(err.Error())
		return err
	}
	b.currentTopology.Store(&hosts)

	currentTopologyCopy := b.GetCurrentTopology()
	if b.collectedTopology.Load() && currentTopologyCopy != nil {
		b.startTopology = currentTopologyCopy
		for _, hostInfo := range currentTopologyCopy {
			b.hostNames.Put(hostInfo.Host, true)
		}
	}
	return nil
}

func (b *BlueGreenStatusMonitor) CollectStatus() {
	connPtr := b.connection.Load()
	if connPtr == nil || b.pluginService.GetTargetDriverDialect().IsClosed(*connPtr) {
		return
	}
	conn := *connPtr

	if !b.blueGreenDialect.IsBlueGreenStatusAvailable(conn) {
		if !b.pluginService.GetTargetDriverDialect().IsClosed(conn) {
			b.currentPhase = driver_infrastructure.NOT_CREATED
			slog.Debug(error_util.GetMessage("BlueGreenDeployment.statusNotAvailable", b.role, driver_infrastructure.NOT_CREATED))
		} else {
			b.connection.Store(nil)
			b.currentPhase = driver_infrastructure.BlueGreenPhase{}
			b.panicMode.Store(true)
		}
		return
	}

	results := b.getBlueGreenStatus(conn)

	statusEntries := make([]StatusInfo, 0, len(results))
	for _, result := range results {
		version := result.Version
		if !slices.Contains(knownVersions, version) {
			versionCopy := version
			version = latestKnownVersion
			slog.Warn(error_util.GetMessage("BlueGreenDeployment.unknownVersion", versionCopy))
		}
		role := driver_infrastructure.ParseRole(result.Role)
		if b.role != role {
			continue
		}
		phase := driver_infrastructure.ParsePhase(result.Status)
		statusEntries = append(statusEntries, StatusInfo{version, result.Endpoint, result.Port, phase, role})
	}

	statusInfo := utils.FilterSliceFindFirst(statusEntries, func(s StatusInfo) bool {
		return utils.IsWriterClusterDns(s.endpoint) && utils.IsNotOldInstance(s.endpoint)
	})

	if !statusInfo.IsZero() {
		// Cluster writer endpoint found. Add reader endpoint as well.
		b.hostNames.Put(strings.Replace(strings.ToLower(statusInfo.endpoint), ".cluster-", ".cluster-ro-", 1), true)
	}

	if statusInfo.IsZero() {
		// Could be an instance endpoint.
		statusInfo = utils.FilterSliceFindFirst(statusEntries, func(s StatusInfo) bool {
			return utils.IsRdsInstance(s.endpoint) && utils.IsNotOldInstance(s.endpoint)
		})
	}

	if statusInfo.IsZero() {
		if len(statusEntries) == 0 {
			// It's normal to expect that the status table has no entries after BGD is completed.
			// Old1 cluster/instance has been separated and no longer receives updates from related green cluster/instance.
			if b.role != driver_infrastructure.SOURCE {
				slog.Warn(error_util.GetMessage("BlueGreenDeployment.noEntriesInStatusTable", b.role))
			}
			b.currentPhase = driver_infrastructure.BlueGreenPhase{}
		}
	} else {
		b.currentPhase = statusInfo.phase
		b.version = statusInfo.version
		b.port = statusInfo.port
	}

	if b.collectedTopology.Load() {
		for _, statusInfo := range statusEntries {
			if statusInfo.endpoint != "" && utils.IsNotOldInstance(statusInfo.endpoint) {
				b.hostNames.Put(strings.ToLower(statusInfo.endpoint), true)
			}
		}
	}

	if !b.connectionHostInfoCorrect.Load() && !statusInfo.IsZero() {
		// We connected to an initialHostInfo that might be not the desired Blue or Green cluster.
		// We need to reconnect to a correct one.
		statusInfoHostIpAddress := b.GetIpAddress(statusInfo.endpoint)
		connectedIpAddressCopy := b.GetConnectedIpAddress()
		if connectedIpAddressCopy != "" && connectedIpAddressCopy != statusInfoHostIpAddress {
			// Found endpoint confirms that we're connected to a different host, and we need to reconnect.
			reconnectionHostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost(statusInfo.endpoint).SetPort(statusInfo.port).Build()
			b.connectionHostInfo.Store(reconnectionHostInfo)
			b.connectionHostInfoCorrect.Store(true)
			b.CloseConnection()
			b.panicMode.Store(true)
		} else {
			// We're already connected to a correct host.
			b.connectionHostInfoCorrect.Store(true)
			b.panicMode.Store(false)
		}

		if b.connectionHostInfoCorrect.Load() && b.hostListProvider == nil {
			// A connection to a correct cluster (blue or green) is established.
			// Let's initialize HostListProvider
			b.InitHostListProvider()
		}
	}
}

func (b *BlueGreenStatusMonitor) InitHostListProvider() {
	if b.hostListProvider != nil || !b.connectionHostInfoCorrect.Load() {
		return
	}

	hostListProps := utils.NewRWMapFromCopy(b.props)

	// Need to instantiate a separate HostListProvider with
	// a special unique clusterId to avoid interference with other HostListProviders opened for this cluster.
	// Blue and Green clusters are expected to have different clusterId.
	clusterId := fmt.Sprintf("%s::%v::%s", b.bgId, b.role, BG_CLUSTER_ID)
	hostListProps.Put(property_util.CLUSTER_ID.Name, clusterId)
	if hostInfo := b.connectionHostInfo.Load(); !hostInfo.IsNil() && b.connectionHostInfoCorrect.Load() {
		hostListProps.Put(property_util.PORT.Name, strconv.Itoa(hostInfo.Port))
		hostListProps.Put(property_util.HOST.Name, hostInfo.Host)
	}
	slog.Debug(error_util.GetMessage("BlueGreenDeployment.createHostListProvider", b.role, clusterId))
	b.hostListProvider = b.pluginService.CreateHostListProvider(hostListProps)
}

func (b *BlueGreenStatusMonitor) OpenConnection() {
	conn := b.connection.Load()
	if conn != nil && !b.pluginService.GetTargetDriverDialect().IsClosed(*conn) {
		return
	}
	b.connection.Store(nil)
	b.panicMode.Store(true)

	connectionHostInfoCopy := b.connectionHostInfo.Load()
	connectedIpAddressCopy := b.GetConnectedIpAddress()

	if connectionHostInfoCopy == nil {
		b.connectionHostInfo.Store(b.initialHostInfo)
		connectionHostInfoCopy = b.initialHostInfo
		b.connectedIpAddress.Store("")
		connectedIpAddressCopy = ""
		b.connectionHostInfoCorrect.Store(false)
	}

	if b.useIpAddress.Load() && connectedIpAddressCopy != "" {
		if connectionWithIpHostInfo, err := host_info_util.NewHostInfoBuilder().CopyFrom(connectionHostInfoCopy).SetHost(connectedIpAddressCopy).Build(); err == nil {
			slog.Debug(error_util.GetMessage("BlueGreenDeployment.openingConnectionWithIp", b.role, connectionHostInfoCopy.Host))
			connectWithIpProps := utils.NewRWMapFromCopy(b.props)
			connectWithIpProps.Put(property_util.IAM_HOST.Name, connectionHostInfoCopy.Host)
			if conn, err := b.pluginService.ForceConnect(connectionWithIpHostInfo, connectWithIpProps); err == nil {
				b.connection.Store(&conn)
				slog.Debug(error_util.GetMessage("BlueGreenDeployment.openedConnectionWithIp", b.role, connectionHostInfoCopy.Host))
				b.panicMode.Store(false)
				return
			}
		}
	} else {
		if finalConnectionHostInfoCopy, err := host_info_util.NewHostInfoBuilder().CopyFrom(connectionHostInfoCopy).Build(); err == nil {
			slog.Debug(error_util.GetMessage("BlueGreenDeployment.openingConnection", b.role, finalConnectionHostInfoCopy.Host))
			connectedIpAddressCopy = b.GetIpAddress(connectionHostInfoCopy.Host)
			if conn, err := b.pluginService.ForceConnect(finalConnectionHostInfoCopy, b.props); err == nil {
				b.connection.Store(&conn)
				slog.Debug(error_util.GetMessage("BlueGreenDeployment.openedConnection", b.role, finalConnectionHostInfoCopy.Host))
				b.connectedIpAddress.Store(connectedIpAddressCopy)
				b.panicMode.Store(false)
				return
			}
		}
	}
	// Can't open connection.
	b.connection.Store(nil)
	b.panicMode.Store(true)
}

func (b *BlueGreenStatusMonitor) CloseConnection() {
	conn := b.connection.Load()
	b.connection.Store(nil)

	if conn != nil && !b.pluginService.GetTargetDriverDialect().IsClosed(*conn) {
		_ = (*conn).Close()
	}
}

func (b *BlueGreenStatusMonitor) GetIpAddress(host string) string {
	ips, err := net.LookupIP(host)
	if err != nil || len(ips) == 0 {
		return ""
	}

	return ips[0].String()
}

func (b *BlueGreenStatusMonitor) GetCurrentTopology() []*host_info_util.HostInfo {
	topology := b.currentTopology.Load()
	if topology == nil {
		return []*host_info_util.HostInfo{}
	}
	return *topology
}

func (b *BlueGreenStatusMonitor) GetConnectedIpAddress() (connectedIpAddressCopy string) {
	if val := b.connectedIpAddress.Load(); val != nil {
		if ip, ok := val.(string); ok {
			connectedIpAddressCopy = ip
		}
	}
	return
}

func (b *BlueGreenStatusMonitor) ResetCollectedData() {
	b.startIpAddressesByHostMap.Clear()
	b.startTopology = make([]*host_info_util.HostInfo, 0)
	b.hostNames.Clear()
}
