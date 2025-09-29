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

package driver_infrastructure

import (
	"database/sql/driver"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"

	"github.com/google/uuid"
)

var highRefreshPeriodAfterPanicNano = time.Second * 30
var ignoreTopologyRequestNano = time.Second * 10
var FallbackTopologyRefreshTimeoutMs = 1100
var topologyUpdateWaitTime = time.Millisecond * 1000

type ConnectionContainer struct {
	Conn driver.Conn
}

var emptyContainer = ConnectionContainer{}

type topologyMapEntry struct {
	id       string
	topology []*host_info_util.HostInfo
}

type ClusterTopologyMonitor interface {
	SetClusterId(clusterId string)
	ForceRefreshVerifyWriter(writerImportant bool, timeoutMs int) ([]*host_info_util.HostInfo, error)
	ForceRefreshUsingConn(conn driver.Conn, timeoutMs int) ([]*host_info_util.HostInfo, error)
	Close()
	Start(wg *sync.WaitGroup)
}

type ClusterTopologyMonitorImpl struct {
	hostListProvider                        *MonitoringRdsHostListProvider
	databaseDialect                         TopologyAwareDialect
	clusterId                               string
	isVerifiedWriterConn                    bool
	highRefreshRateEndTimeInNanos           int64
	highRefreshRateNano                     time.Duration
	refreshRateNano                         time.Duration
	topologyCacheExpirationNano             time.Duration
	topologyMap                             *utils.CacheMap[topologyMapEntry]
	monitoringProps                         *utils.RWMap[string]
	initialHostInfo                         *host_info_util.HostInfo
	clusterInstanceTemplate                 *host_info_util.HostInfo
	pluginService                           PluginService
	hostRoutines                            *sync.Map
	hostRoutinesWg                          sync.WaitGroup
	stop                                    atomic.Bool
	ignoreNewTopologyRequestsEndTimeInNanos atomic.Int64
	requestToUpdateTopology                 atomic.Bool
	requestToUpdateTopologyChannel          chan bool
	topologyUpdatedChannel                  chan bool
	hostRoutinesStop                        atomic.Bool
	monitoringConn                          atomic.Value
	hostRoutinesWriterConn                  atomic.Value
	hostRoutinesReaderConn                  atomic.Value
	hostRoutinesLatestTopology              atomic.Value
	hostRoutinesWriterHostInfo              atomic.Pointer[host_info_util.HostInfo]
	writerHostInfo                          atomic.Pointer[host_info_util.HostInfo]
}

func NewClusterTopologyMonitorImpl(
	hostListProvider *MonitoringRdsHostListProvider,
	dialect TopologyAwareDialect,
	clusterId string,
	highRefreshRateNano time.Duration,
	refreshRateNano time.Duration,
	topologyCacheExpirationNano time.Duration,
	props *utils.RWMap[string],
	initialHostInfo *host_info_util.HostInfo,
	clusterInstanceTemplate *host_info_util.HostInfo,
	pluginService PluginService) *ClusterTopologyMonitorImpl {
	return &ClusterTopologyMonitorImpl{
		hostListProvider:               hostListProvider,
		databaseDialect:                dialect,
		clusterId:                      clusterId,
		monitoringProps:                props,
		initialHostInfo:                initialHostInfo,
		clusterInstanceTemplate:        clusterInstanceTemplate,
		pluginService:                  pluginService,
		hostRoutines:                   &sync.Map{},
		topologyMap:                    utils.NewCache[topologyMapEntry](),
		highRefreshRateNano:            highRefreshRateNano,
		refreshRateNano:                refreshRateNano,
		topologyCacheExpirationNano:    topologyCacheExpirationNano,
		requestToUpdateTopologyChannel: make(chan bool),
		topologyUpdatedChannel:         make(chan bool),
	}
}

func (c *ClusterTopologyMonitorImpl) Start(wg *sync.WaitGroup) {
	c.monitoringConn.Store(emptyContainer)
	c.hostRoutinesWriterConn.Store(emptyContainer)
	c.hostRoutinesReaderConn.Store(emptyContainer)
	go c.Run(wg)
}

func (c *ClusterTopologyMonitorImpl) SetClusterId(clusterId string) {
	c.clusterId = clusterId
}

func (c *ClusterTopologyMonitorImpl) loadConn(conn atomic.Value) driver.Conn {
	value := conn.Load()
	if value == nil {
		// Nothing has been stored.
		return nil
	}
	connContainer, ok := value.(ConnectionContainer)
	if !ok {
		// Didn't store a ConnectionContainer, should not occur.
		return nil
	}
	return connContainer.Conn
}

func (c *ClusterTopologyMonitorImpl) ForceRefreshVerifyWriter(shouldVerify bool, timeoutMs int) ([]*host_info_util.HostInfo, error) {
	if c.ignoreNewTopologyRequestsEndTimeInNanos.Load() > 0 && time.Now().Before(time.Unix(0, c.ignoreNewTopologyRequestsEndTimeInNanos.Load())) {
		// Previous failover has just completed. We can use results of it without triggering a new topology update.
		mapEntry, ok := c.topologyMap.Get(c.clusterId)
		slog.Debug(utils.LogTopology(mapEntry.topology, error_util.GetMessage("ClusterTopologyMonitorImpl.ignoringTopologyRequest")))
		if ok && len(mapEntry.topology) > 0 {
			return mapEntry.topology, nil
		}
	}

	if shouldVerify {
		monitoringConn := c.loadConn(c.monitoringConn)
		c.monitoringConn.Store(emptyContainer)
		c.isVerifiedWriterConn = false
		c.closeConnection(monitoringConn)
	}

	return c.waitTillTopologyGetsUpdated(timeoutMs)
}

func (c *ClusterTopologyMonitorImpl) ForceRefreshUsingConn(conn driver.Conn, timeoutMs int) ([]*host_info_util.HostInfo, error) {
	if c.isVerifiedWriterConn {
		// Push monitoring thread to refresh topology with a verified connection.
		return c.waitTillTopologyGetsUpdated(timeoutMs)
	}

	// Otherwise use provided unverified connection to update topology.
	return c.fetchTopologyAndUpdateCache(conn), nil
}

func (c *ClusterTopologyMonitorImpl) waitTillTopologyGetsUpdated(timeoutMs int) ([]*host_info_util.HostInfo, error) {
	mapEntry, ok := c.topologyMap.Get(c.clusterId)

	// Notify monitoring routines that topology should be refreshed immediately.
	c.requestToUpdateTopology.Store(true)
	c.notifyChannel(c.requestToUpdateTopologyChannel)

	if timeoutMs == 0 && ok && len(mapEntry.topology) > 0 {
		slog.Debug(utils.LogTopology(mapEntry.topology, error_util.GetMessage("ClusterTopologyMonitorImpl.timeoutSetToZero")))
		return mapEntry.topology, nil
	}

	if timeoutMs == 0 {
		timeoutMs = FallbackTopologyRefreshTimeoutMs
	}
	end := time.Now().Add(time.Millisecond * time.Duration(timeoutMs))
	latestMapEntry := mapEntry
	exit := false
	for mapEntry.id == latestMapEntry.id && time.Now().Before(end) && !exit {
		select {
		case <-c.topologyUpdatedChannel:
			exit = true
		default:
			time.Sleep(topologyUpdateWaitTime)
		}
		latestMapEntry, _ = c.topologyMap.Get(c.clusterId)
	}

	if time.Now().After(end) {
		return nil, error_util.NewTimeoutError(error_util.GetMessage("ClusterTopologyMonitorImpl.topologyNotUpdated", timeoutMs))
	}

	return latestMapEntry.topology, nil
}

func (c *ClusterTopologyMonitorImpl) fetchTopologyAndUpdateCache(conn driver.Conn) []*host_info_util.HostInfo {
	if conn == nil {
		return nil
	}

	hosts, err := c.queryForTopology(conn)
	if err != nil {
		// Do nothing.
		slog.Debug(error_util.GetMessage("ClusterTopologyMonitorImpl.errorFetchingTopology", err.Error()))
		return nil
	}

	if len(hosts) != 0 {
		c.updateTopologyCache(hosts)
	}
	return hosts
}

func (c *ClusterTopologyMonitorImpl) queryForTopology(conn driver.Conn) ([]*host_info_util.HostInfo, error) {
	return c.databaseDialect.GetTopology(conn, c.hostListProvider)
}

func (c *ClusterTopologyMonitorImpl) updateTopologyCache(hosts []*host_info_util.HostInfo) {
	c.topologyMap.Put(c.clusterId, topologyMapEntry{uuid.New().String(), hosts}, c.topologyCacheExpirationNano)
	c.requestToUpdateTopology.Store(false)
	c.notifyChannel(c.topologyUpdatedChannel)
}

func (c *ClusterTopologyMonitorImpl) openAnyConnectionAndUpdateTopology() ([]*host_info_util.HostInfo, error) {
	writerVerifiedByThisRoutine := false

	if c.loadConn(c.monitoringConn) == nil {
		// Open a new connection.
		conn, err := c.pluginService.ForceConnect(c.initialHostInfo, c.monitoringProps)
		if err != nil || conn == nil {
			// Can't connect.
			return nil, err
		}

		if c.monitoringConn.CompareAndSwap(emptyContainer, ConnectionContainer{conn}) {
			slog.Debug(error_util.GetMessage("ClusterTopologyMonitorImpl.openedMonitoringConnection", c.initialHostInfo.GetHost()))

			writerId, getWriterNameErr := c.databaseDialect.GetWriterHostName(conn)
			if getWriterNameErr == nil && writerId != "" {
				c.isVerifiedWriterConn = true
				writerVerifiedByThisRoutine = true

				if utils.IsRdsInstance(c.initialHostInfo.GetHost()) {
					c.writerHostInfo.Store(c.initialHostInfo)
					slog.Debug(error_util.GetMessage("ClusterTopologyMonitorImpl.writerMonitoringConnection", c.writerHostInfo.Load().GetHost()))
				} else {
					hostId := c.databaseDialect.GetHostName(c.loadConn(c.monitoringConn))
					if hostId != "" {
						c.writerHostInfo.Store(c.createHost(hostId, true, 0, time.Time{}))
						slog.Debug(error_util.GetMessage("ClusterTopologyMonitorImpl.writerMonitoringConnection", c.writerHostInfo.Load().GetHost()))
					}
				}
			}
		} else {
			// Monitoring connection has already been set by other routine, close new connection as we don't need it.
			c.closeConnection(conn)
		}
	}

	hosts := c.fetchTopologyAndUpdateCache(c.loadConn(c.monitoringConn))
	if writerVerifiedByThisRoutine {
		// We verify the writer on initial connection and on failover, but we only want to ignore new topology
		// requests after failover. To accomplish this, the first time we verify the writer we set the ignore end
		// time to 0. Any future writer verifications will set it to a positive value.
		if !c.ignoreNewTopologyRequestsEndTimeInNanos.CompareAndSwap(-1, 0) {
			c.ignoreNewTopologyRequestsEndTimeInNanos.Store(time.Now().Add(ignoreTopologyRequestNano).Unix())
		}
	}

	if len(hosts) == 0 {
		// Can't get topology, there might be something wrong with a connection. Close connection.
		connToClose := c.loadConn(c.monitoringConn)
		c.monitoringConn.Store(emptyContainer)
		c.closeConnection(connToClose)
		c.isVerifiedWriterConn = false
	}

	return hosts, nil
}

func (c *ClusterTopologyMonitorImpl) Close() {
	// Break waiting/sleeping cycles in monitoring routines.
	c.requestToUpdateTopology.Store(true)
	c.notifyChannel(c.requestToUpdateTopologyChannel)
	c.notifyChannel(c.topologyUpdatedChannel)

	// Signal for monitoring loop to exit.
	c.stop.Store(true)
	c.hostRoutinesStop.Store(true)

	// Waiting to give routines enough time to exit the monitoring loop and close database connections.
	time.Sleep(time.Second * 5)

	c.hostRoutinesWg.Wait()
	c.hostRoutines.Clear()
	close(c.requestToUpdateTopologyChannel)
	close(c.topologyUpdatedChannel)
}

func (c *ClusterTopologyMonitorImpl) isInPanicMode() bool {
	return c.loadConn(c.monitoringConn) == nil || !c.isVerifiedWriterConn
}

func (c *ClusterTopologyMonitorImpl) delay(useHighRefreshRate bool) {
	if c.highRefreshRateEndTimeInNanos > 0 && time.Now().Unix() < c.highRefreshRateEndTimeInNanos {
		useHighRefreshRate = true
	}

	if c.requestToUpdateTopology.Load() {
		useHighRefreshRate = true
	}

	var durationNanos time.Duration
	if useHighRefreshRate {
		durationNanos = c.highRefreshRateNano
	} else {
		durationNanos = c.refreshRateNano
	}
	start := time.Now()
	end := start.Add(durationNanos)
	for ok := true; ok; ok = time.Now().Before(end) && !c.stop.Load() && !c.requestToUpdateTopology.Load() {
		select {
		case <-c.requestToUpdateTopologyChannel:
			return
		default:
			time.Sleep(time.Millisecond * 50)
		}
	}
}

func (c *ClusterTopologyMonitorImpl) closeConnection(conn driver.Conn) {
	if conn != nil && !c.pluginService.GetTargetDriverDialect().IsClosed(conn) {
		_ = conn.Close()
	}
}

func (c *ClusterTopologyMonitorImpl) createHost(hostName string, isWriter bool, weight int, lastUpdateTime time.Time) *host_info_util.HostInfo {
	if hostName == "" {
		hostName = "?"
	}

	endpoint := c.getHostEndpoint(hostName)
	port := c.clusterInstanceTemplate.Port
	if port == host_info_util.HOST_NO_PORT {
		if c.initialHostInfo.IsPortSpecified() {
			port = c.initialHostInfo.Port
		} else {
			port = c.hostListProvider.databaseDialect.GetDefaultPort()
		}
	}

	var role host_info_util.HostRole
	if isWriter {
		role = host_info_util.WRITER
	} else {
		role = host_info_util.READER
	}

	hostInfoBuilder := host_info_util.NewHostInfoBuilder()
	hostInfo, err := hostInfoBuilder.
		SetHost(endpoint).
		SetHostId(hostName).
		SetPort(port).
		SetRole(role).
		SetAvailability(host_info_util.AVAILABLE).
		SetWeight(weight).
		SetLastUpdateTime(lastUpdateTime).
		Build()
	if err == nil {
		hostInfo.AddAlias(hostName)
	}
	return hostInfo
}

func (c *ClusterTopologyMonitorImpl) getHostEndpoint(hostName string) string {
	host := c.clusterInstanceTemplate.Host
	return strings.Replace(host, "?", hostName, 1)
}

func (c *ClusterTopologyMonitorImpl) notifyChannel(channel chan bool) {
	if !c.stop.Load() {
		select {
		case channel <- true:
		default:
		}
	}
}

func (c *ClusterTopologyMonitorImpl) Run(wg *sync.WaitGroup) {
	slog.Debug(error_util.GetMessage("ClusterTopologyMonitorImpl.startMonitoringRoutine", c.initialHostInfo.GetHost()))

	for !c.stop.Load() {
		if c.isInPanicMode() {
			if utils.LengthOfSyncMap(c.hostRoutines) == 0 {
				slog.Debug(error_util.GetMessage("ClusterTopologyMonitorImpl.startingHostMonitoringRoutines"))

				// Start host routines
				c.hostRoutinesStop.Store(false)
				c.hostRoutinesWriterConn.Store(emptyContainer)
				c.hostRoutinesReaderConn.Store(emptyContainer)
				c.hostRoutinesWriterHostInfo.Store(nil)
				c.hostRoutinesLatestTopology.Store(map[string][]*host_info_util.HostInfo{})

				mapEntry, ok := c.topologyMap.Get(c.clusterId)
				hosts := mapEntry.topology
				if !ok || len(hosts) == 0 {
					// Need any connection to get topology.
					hosts, _ = c.openAnyConnectionAndUpdateTopology()
				}

				if len(hosts) != 0 && !c.isVerifiedWriterConn {
					for _, hostInfo := range hosts {
						hostMonitor := &HostMonitoringRoutine{
							monitor:        c,
							hostInfo:       hostInfo,
							writerHostInfo: c.writerHostInfo.Load(),
						}
						hostMonitor.Init()
						c.hostRoutinesWg.Add(1)
						c.hostRoutines.Store(hostInfo.Host, hostMonitor)
					}
				}

				// Otherwise let's try it again the next round.
			} else {
				// Host routines are running.
				// Check if writer is already detected.
				writerConn := c.loadConn(c.hostRoutinesWriterConn)
				writerConnHostInfo := c.hostRoutinesWriterHostInfo.Load()

				if writerConn != nil && !writerConnHostInfo.IsNil() {
					slog.Debug(error_util.GetMessage("ClusterTopologyMonitorImpl.writerPickedUpFromHostMonitors", writerConnHostInfo.String()))
					c.closeConnection(c.loadConn(c.monitoringConn))
					c.monitoringConn.Store(ConnectionContainer{writerConn})
					c.writerHostInfo.Store(writerConnHostInfo)
					c.isVerifiedWriterConn = true
					c.highRefreshRateEndTimeInNanos = time.Now().Add(highRefreshPeriodAfterPanicNano).Unix()

					// We verify the writer on initial connection and on failover, but we only want to ignore new topology
					// requests after failover. To accomplish this, the first time we verify the writer we set the ignore end
					// time to 0. Any future writer verifications will set it to a positive value.
					if !c.ignoreNewTopologyRequestsEndTimeInNanos.CompareAndSwap(-1, 0) {
						c.ignoreNewTopologyRequestsEndTimeInNanos.Store(time.Now().Add(ignoreTopologyRequestNano).Unix())
					}

					c.hostRoutinesStop.Store(true)
					c.hostRoutinesWg.Wait()
					c.hostRoutines.Clear()
					continue
				} else {
					// Update host routines with new hosts in the topology.
					hosts, ok := c.hostRoutinesLatestTopology.Load().([]*host_info_util.HostInfo)
					if ok && len(hosts) > 0 && !c.hostRoutinesStop.Load() {
						for _, hostInfo := range hosts {
							_, foundHostRoutine := c.hostRoutines.Load(hostInfo.Host)
							if !foundHostRoutine {
								hostMonitor := &HostMonitoringRoutine{
									monitor:        c,
									hostInfo:       hostInfo,
									writerHostInfo: c.writerHostInfo.Load(),
								}
								hostMonitor.Init()
								c.hostRoutines.Store(hostInfo.Host, hostMonitor)
							}
						}
					}
				}
			}

			c.delay(true)
		} else {
			// Regular mode (not panic mode).

			if utils.LengthOfSyncMap(c.hostRoutines) != 0 {
				c.hostRoutinesStop.Store(true)
				c.hostRoutinesWg.Wait()
				c.hostRoutines.Clear()
			}

			hosts := c.fetchTopologyAndUpdateCache(c.loadConn(c.monitoringConn))
			if len(hosts) == 0 {
				// Can't get topology, switch to panic mode.
				conn := c.loadConn(c.monitoringConn)
				c.monitoringConn.Store(emptyContainer)
				c.isVerifiedWriterConn = false
				if conn != nil {
					_ = conn.Close()
				}
				continue
			}

			if c.highRefreshRateEndTimeInNanos > 0 && time.Now().Unix() > c.highRefreshRateEndTimeInNanos {
				c.highRefreshRateEndTimeInNanos = 0
			}

			// Do not log topology while in high refresh rate.
			if c.highRefreshRateEndTimeInNanos == 0 {
				mapEntry, ok := c.topologyMap.Get(c.clusterId)
				if ok {
					slog.Debug(utils.LogTopology(mapEntry.topology, ""))
				}
			}

			c.delay(false)
		}
	}

	wg.Done()
}

type HostMonitoringRoutine struct {
	monitor        *ClusterTopologyMonitorImpl
	hostInfo       *host_info_util.HostInfo
	writerHostInfo *host_info_util.HostInfo
	writerChanged  bool
}

func (h *HostMonitoringRoutine) Init() {
	go h.run()
}

func (h *HostMonitoringRoutine) run() {
	var conn driver.Conn
	var err error
	updateTopology := false
	startTime := time.Now()
	defer func() {
		h.monitor.closeConnection(conn)
		end := time.Now()
		slog.Debug(error_util.GetMessage("HostMonitoringRoutine.routineCompleted", end.Sub(startTime)))
		h.monitor.hostRoutinesWg.Done()
	}()

	for !h.monitor.hostRoutinesStop.Load() {
		if conn == nil {
			conn, err = h.monitor.pluginService.ForceConnect(h.hostInfo, h.monitor.monitoringProps)
			if err != nil {
				// Connect issues.
				h.monitor.pluginService.SetAvailability(h.hostInfo.AllAliases, host_info_util.UNAVAILABLE)
			} else {
				h.monitor.pluginService.SetAvailability(h.hostInfo.AllAliases, host_info_util.AVAILABLE)
			}
		}

		if conn != nil {
			writerId := ""
			writerId, err = h.monitor.databaseDialect.GetWriterHostName(conn)
			if err != nil {
				h.monitor.closeConnection(conn)
				conn = nil
			}

			if writerId != "" {
				// This prevents closing connection in run cleanup.
				if !h.monitor.hostRoutinesWriterConn.CompareAndSwap(emptyContainer, ConnectionContainer{conn}) {
					// Writer connection is already set up.
					h.monitor.closeConnection(conn)
				} else {
					// Writer connection is successfully set to writerConn
					slog.Debug(error_util.GetMessage("HostMonitoringRoutine.detectedWriter", writerId))
					// When hostRoutinesWriterConn and hostRoutinesWriterHostInfo are both set, the topology monitor may
					// set ignoreNewTopologyRequestsEndTimeInNanos, in which case other routines will use the cached topology
					// for the ignore duration, so we need to update the topology before setting hostRoutinesWriterHostInfo.
					h.monitor.fetchTopologyAndUpdateCache(conn)
					h.monitor.hostRoutinesWriterHostInfo.Store(h.hostInfo)
					h.monitor.hostRoutinesStop.Store(true)
					mapEntry, _ := h.monitor.topologyMap.Get(h.monitor.clusterId)
					slog.Debug(utils.LogTopology(mapEntry.topology, ""))
				}

				// Setting the connection to nil here prevents the defer from closing hostRoutinesWriterConn.
				conn = nil
				return
			} else {
				// This connection is a reader connection.
				if h.monitor.loadConn(h.monitor.hostRoutinesWriterConn) != nil {
					// While writer connection isn't yet established this reader connection may update topology.
					if updateTopology {
						h.readerRoutineFetchTopology(conn, *h.writerHostInfo)
					} else if h.monitor.loadConn(h.monitor.hostRoutinesReaderConn) != nil {
						if h.monitor.hostRoutinesReaderConn.CompareAndSwap(emptyContainer, ConnectionContainer{conn}) {
							// Let's use this connection to update topology.
							updateTopology = true
							h.readerRoutineFetchTopology(conn, *h.writerHostInfo)
						}
					}
				}
			}
		}

		time.Sleep(time.Millisecond * 100)
	}
}

func (h *HostMonitoringRoutine) readerRoutineFetchTopology(conn driver.Conn, writerHostInfo host_info_util.HostInfo) {
	if conn == nil {
		return
	}

	hosts, err := h.monitor.queryForTopology(conn)
	if len(hosts) == 0 || err != nil {
		return
	}

	// Share this topology so the main monitoring routine be able to adjust host monitoring routines.
	h.monitor.hostRoutinesLatestTopology.Store(hosts)

	if h.writerChanged {
		h.monitor.updateTopologyCache(hosts)
		slog.Debug(utils.LogTopology(hosts, ""))
		return
	}

	latestWriterHostInfo := host_info_util.GetWriter(hosts)
	if !latestWriterHostInfo.IsNil() && !writerHostInfo.IsNil() && latestWriterHostInfo.GetHostAndPort() != writerHostInfo.GetHostAndPort() {
		// Writer host has changed.
		h.writerChanged = true

		slog.Debug(error_util.GetMessage("HostMonitoringRoutine.writerHostChanged", writerHostInfo.GetHost(), latestWriterHostInfo.GetHost()))

		// We can update topology cache and notify all waiting routines.
		h.monitor.updateTopologyCache(hosts)
		slog.Debug(utils.LogTopology(hosts, ""))
	}
}
