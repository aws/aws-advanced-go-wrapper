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
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

var highRefreshPeriodAfterPanicNano = time.Second * 30
var FallbackTopologyRefreshTimeoutMs = 1100
var topologyUpdateWaitTime = time.Millisecond * 1000

type ConnectionContainer struct {
	Conn driver.Conn
}

var emptyContainer = ConnectionContainer{}

type ClusterTopologyMonitor interface {
	Monitor
	ForceRefreshVerifyWriter(writerImportant bool, timeoutMs int) ([]*host_info_util.HostInfo, error)
	ForceRefreshUsingConn(conn driver.Conn, timeoutMs int) ([]*host_info_util.HostInfo, error)
}

// ClusterTopologyMonitorType is the type descriptor for cluster topology monitors.
// Used with MonitorService to manage ClusterTopologyMonitor instances.
var ClusterTopologyMonitorType = &MonitorType{Name: "ClusterTopologyMonitor"}

type ClusterTopologyMonitorImpl struct {
	servicesContainer              ServicesContainer
	topologyUtils                  TopologyUtils
	clusterId                      string
	isVerifiedWriterConn           bool
	highRefreshRateEndTimeInNanos  int64
	highRefreshRateNano            time.Duration
	refreshRateNano                time.Duration
	topologyCacheExpirationNano    time.Duration
	monitoringProps                *utils.RWMap[string, string]
	initialHostInfo                *host_info_util.HostInfo
	clusterInstanceTemplate        *host_info_util.HostInfo
	pluginService                  PluginService
	hostRoutines                   *sync.Map
	hostRoutinesWg                 sync.WaitGroup
	stop                           atomic.Bool
	requestToUpdateTopology        atomic.Bool
	requestToUpdateTopologyChannel chan bool
	topologyUpdatedChannel         chan bool
	hostRoutinesStop               atomic.Bool
	monitoringConn                 atomic.Value
	hostRoutinesWriterConn         atomic.Value
	hostRoutinesReaderConn         atomic.Value
	hostRoutinesLatestTopology     atomic.Value
	hostRoutinesWriterHostInfo     atomic.Pointer[host_info_util.HostInfo]
	writerHostInfo                 atomic.Pointer[host_info_util.HostInfo]
	state                          atomic.Value // MonitorState
	lastActivityTimestampNano      atomic.Int64
	stopCh                         chan struct{} // For clean shutdown
	wg                             sync.WaitGroup
}

func NewClusterTopologyMonitorImpl(
	servicesContainer ServicesContainer,
	topologyUtils TopologyUtils,
	clusterId string,
	highRefreshRateNano time.Duration,
	refreshRateNano time.Duration,
	topologyCacheExpirationNano time.Duration,
	props *utils.RWMap[string, string],
	initialHostInfo *host_info_util.HostInfo,
	clusterInstanceTemplate *host_info_util.HostInfo,
	pluginService PluginService) *ClusterTopologyMonitorImpl {
	return &ClusterTopologyMonitorImpl{
		servicesContainer:              servicesContainer,
		topologyUtils:                  topologyUtils,
		clusterId:                      clusterId,
		monitoringProps:                props,
		initialHostInfo:                initialHostInfo,
		clusterInstanceTemplate:        clusterInstanceTemplate,
		pluginService:                  pluginService,
		hostRoutines:                   &sync.Map{},
		highRefreshRateNano:            highRefreshRateNano,
		refreshRateNano:                refreshRateNano,
		topologyCacheExpirationNano:    topologyCacheExpirationNano,
		requestToUpdateTopologyChannel: make(chan bool),
		topologyUpdatedChannel:         make(chan bool),
	}
}

func (c *ClusterTopologyMonitorImpl) Start() {
	c.state.Store(MonitorStateRunning)
	c.monitoringConn.Store(emptyContainer)
	c.hostRoutinesWriterConn.Store(emptyContainer)
	c.hostRoutinesReaderConn.Store(emptyContainer)
	c.wg.Add(1)
	c.lastActivityTimestampNano.Store(time.Now().UnixNano())
	go func() {
		defer c.wg.Done()
		c.Monitor()
	}()
}

func (c *ClusterTopologyMonitorImpl) Monitor() {
	slog.Debug(error_util.GetMessage("ClusterTopologyMonitorImpl.startMonitoringRoutine", c.initialHostInfo.GetHost()))
	// TODO: subscribe to event publisher
	for !c.stop.Load() {
		c.lastActivityTimestampNano.Store(time.Now().UnixNano())
		if c.isInPanicMode() {
			if utils.LengthOfSyncMap(c.hostRoutines) == 0 {
				slog.Debug(error_util.GetMessage("ClusterTopologyMonitorImpl.startingHostMonitoringRoutines"))

				// Start host routines
				c.hostRoutinesStop.Store(false)
				c.hostRoutinesWriterConn.Store(emptyContainer)
				c.hostRoutinesReaderConn.Store(emptyContainer)
				c.hostRoutinesWriterHostInfo.Store(nil)
				c.hostRoutinesLatestTopology.Store(map[string][]*host_info_util.HostInfo{})

				topology, ok := TopologyStorageType.Get(c.servicesContainer.GetStorageService(), c.clusterId)
				hosts := topology.GetHosts()
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
				topology, ok := TopologyStorageType.Get(c.servicesContainer.GetStorageService(), c.clusterId)
				if ok {
					slog.Debug(utils.LogTopology(topology.GetHosts(), ""))
				}
			}

			c.delay(false)
		}
	}
	c.state.Store(MonitorStateStopped)
}

func (c *ClusterTopologyMonitorImpl) Stop() {
	c.stop.Store(true)
	c.hostRoutinesStop.Store(true)
	// signal channels to unblock waiting - send directly since stop is already true
	select {
	case c.requestToUpdateTopologyChannel <- true:
	default:
	}
	select {
	case c.topologyUpdatedChannel <- true:
	default:
	}
	// Wait for Monitor() to finish
	c.wg.Wait()
	c.Close()
}

func (c *ClusterTopologyMonitorImpl) Close() {
	c.hostRoutinesWg.Wait()
	c.hostRoutines.Clear()
	c.closeConnection(c.loadConn(c.monitoringConn))
	// Close channels safely - they may already be closed
	defer func() { recover() }()
	close(c.requestToUpdateTopologyChannel)
	close(c.topologyUpdatedChannel)
}

func (c *ClusterTopologyMonitorImpl) GetLastActivityTimestampNanos() int64 {
	return c.lastActivityTimestampNano.Load()
}

func (c *ClusterTopologyMonitorImpl) GetState() MonitorState {
	if state := c.state.Load(); state != nil {
		return state.(MonitorState)
	}
	return MonitorStateStopped
}

func (c *ClusterTopologyMonitorImpl) CanDispose() bool {
	return true
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
	if shouldVerify {
		monitoringConn := c.loadConn(c.monitoringConn)
		c.monitoringConn.Store(emptyContainer)
		c.isVerifiedWriterConn = false
		c.closeConnection(monitoringConn)
	}

	return c.waitForTopologyUpdate(timeoutMs)
}

func (c *ClusterTopologyMonitorImpl) getStoredHosts() []*host_info_util.HostInfo {
	topology := c.getStoredTopology()
	if topology == nil {
		return nil
	}
	return topology.GetHosts()
}

func (c *ClusterTopologyMonitorImpl) getStoredTopology() *Topology {
	topology, found := TopologyStorageType.Get(c.servicesContainer.GetStorageService(), c.clusterId)
	if !found {
		return nil
	}
	return topology
}

func (c *ClusterTopologyMonitorImpl) ForceRefreshUsingConn(conn driver.Conn, timeoutMs int) ([]*host_info_util.HostInfo, error) {
	if c.isVerifiedWriterConn {
		// Push monitoring thread to refresh topology with a verified connection.
		return c.waitForTopologyUpdate(timeoutMs)
	}

	// Otherwise use provided unverified connection to update topology.
	return c.fetchTopologyAndUpdateCache(conn), nil
}

func (c *ClusterTopologyMonitorImpl) waitForTopologyUpdate(timeoutMs int) ([]*host_info_util.HostInfo, error) {
	currentTopology := c.getStoredTopology()

	// Notify monitoring routines that topology should be refreshed immediately.
	c.requestToUpdateTopology.Store(true)
	c.notifyChannel(c.requestToUpdateTopologyChannel)

	currentHosts := c.getStoredHosts()
	if timeoutMs == 0 {
		slog.Debug(utils.LogTopology(currentHosts, error_util.GetMessage("ClusterTopologyMonitorImpl.timeoutSetToZero")))
		return currentHosts, nil
	}

	end := time.Now().Add(time.Millisecond * time.Duration(timeoutMs))

	// Note: we are checking reference equality instead of value equality.
	// We will break out of the loop if there is a new entry in the topology cache,
	// even if the value of the hosts in latestTopology is the same as currentTopology.
	var latestTopology *Topology
	for {
		latestTopology = c.getStoredTopology()
		if currentTopology != latestTopology || time.Now().After(end) {
			break
		}

		select {
		case <-c.topologyUpdatedChannel:
			// Topology was updated, check again
		case <-time.After(topologyUpdateWaitTime):
			// Timeout on wait, check again
		}
	}

	if time.Now().After(end) {
		return nil, error_util.NewTimeoutError(error_util.GetMessage("ClusterTopologyMonitorImpl.topologyNotUpdated", timeoutMs))
	}

	if latestTopology == nil {
		return nil, nil
	}
	return latestTopology.GetHosts(), nil
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
	return c.topologyUtils.QueryForTopology(conn, c.initialHostInfo, c.clusterInstanceTemplate)
}

func (c *ClusterTopologyMonitorImpl) updateTopologyCache(hosts []*host_info_util.HostInfo) {
	// c.topologyMap.Put(c.clusterId, topologyMapEntry{uuid.New().String(), hosts}, c.topologyCacheExpirationNano)
	TopologyStorageType.Set(c.servicesContainer.GetStorageService(), c.clusterId, NewTopology(hosts))
	c.requestToUpdateTopology.Store(false)
	c.notifyChannel(c.topologyUpdatedChannel)
}

func (c *ClusterTopologyMonitorImpl) openAnyConnectionAndUpdateTopology() ([]*host_info_util.HostInfo, error) {
	if c.loadConn(c.monitoringConn) == nil {
		// Open a new connection.
		conn, err := c.pluginService.ForceConnect(c.initialHostInfo, c.monitoringProps)
		if err != nil || conn == nil {
			// Can't connect.
			return nil, err
		}

		if c.monitoringConn.CompareAndSwap(emptyContainer, ConnectionContainer{conn}) {
			slog.Debug(error_util.GetMessage("ClusterTopologyMonitorImpl.openedMonitoringConnection", c.initialHostInfo.GetHost()))

			isWriterInstance, getWriterNameErr := c.topologyUtils.IsWriterInstance(conn)
			if getWriterNameErr == nil && isWriterInstance {
				c.isVerifiedWriterConn = true

				if utils.IsRdsInstance(c.initialHostInfo.GetHost()) {
					c.writerHostInfo.Store(c.initialHostInfo)
					slog.Debug(error_util.GetMessage("ClusterTopologyMonitorImpl.writerMonitoringConnection", c.writerHostInfo.Load().GetHost()))
				} else {
					hostId, hostName := c.topologyUtils.GetInstanceId(conn)
					if hostId != "" || hostName != "" {
						c.writerHostInfo.Store(c.topologyUtils.CreateHost(hostId, hostName, true, 0, time.Time{}, c.initialHostInfo, c.clusterInstanceTemplate))
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

	if len(hosts) == 0 {
		// Can't get topology, there might be something wrong with a connection. Close connection.
		connToClose := c.loadConn(c.monitoringConn)
		c.monitoringConn.Store(emptyContainer)
		c.closeConnection(connToClose)
		c.isVerifiedWriterConn = false
	}

	return hosts, nil
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

func (c *ClusterTopologyMonitorImpl) notifyChannel(channel chan bool) {
	if !c.stop.Load() {
		select {
		case channel <- true:
		default:
		}
	}
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
			isWriter, err := h.monitor.topologyUtils.IsWriterInstance(conn)
			if err != nil {
				h.monitor.closeConnection(conn)
				conn = nil
			}

			if isWriter {
				// This prevents closing connection in run cleanup.
				if !h.monitor.hostRoutinesWriterConn.CompareAndSwap(emptyContainer, ConnectionContainer{conn}) {
					// Writer connection is already set up.
					h.monitor.closeConnection(conn)
				} else {
					// Writer connection is successfully set to writerConn
					slog.Debug(error_util.GetMessage("HostMonitoringRoutine.detectedWriter"))
					// We need to update the topology before setting hostRoutinesWriterHostInfo
					// so that the topology is available when the monitor picks up the writer.
					h.monitor.fetchTopologyAndUpdateCache(conn)
					h.monitor.hostRoutinesWriterHostInfo.Store(h.hostInfo)
					h.monitor.hostRoutinesStop.Store(true)
					hosts := h.monitor.getStoredHosts()
					slog.Debug(utils.LogTopology(hosts, ""))
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
