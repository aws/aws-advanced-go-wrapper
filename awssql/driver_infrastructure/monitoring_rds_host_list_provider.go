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
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"sync"
	"time"
)

var MONITOR_EXPIRATION_NANOS = time.Minute
var TOPOLOGY_CACHE_EXPIRATION_NANO = time.Minute * 5
var DEFAULT_TOPOLOGY_QUERY_TIMEOUT_MS = 5000

var clusterTopologyMonitors *utils.SlidingExpirationCache[ClusterTopologyMonitor]
var clusterTopologyMonitorsMutex sync.Mutex
var clusterTopologyMonitorWg = &sync.WaitGroup{}

type MonitoringRdsHostListProvider struct {
	defaultTopologyQueryTimeoutMs int
	pluginService                 PluginService
	*RdsHostListProvider
}

func MonitoringRdsHostListProviderClearCaches() {
	if clusterTopologyMonitors != nil {
		clusterTopologyMonitors.Clear()
		clusterTopologyMonitorWg.Wait()
	}
}

func NewMonitoringRdsHostListProvider(
	hostListProviderService HostListProviderService,
	databaseDialect TopologyAwareDialect,
	properties map[string]string,
	originalDsn string,
	pluginService PluginService) *MonitoringRdsHostListProvider {
	clusterTopologyMonitorsMutex.Lock()
	if clusterTopologyMonitors == nil {
		var disposalFunc utils.DisposalFunc[ClusterTopologyMonitor] = func(item ClusterTopologyMonitor) bool {
			item.Close()
			return true
		}
		clusterTopologyMonitors = utils.NewSlidingExpirationCache[ClusterTopologyMonitor]("cluster-topology-monitors", disposalFunc)
		clusterTopologyMonitors.SetCleanupIntervalNanos(MONITOR_EXPIRATION_NANOS)
	}
	clusterTopologyMonitorsMutex.Unlock()

	m := &MonitoringRdsHostListProvider{
		defaultTopologyQueryTimeoutMs: DEFAULT_TOPOLOGY_QUERY_TIMEOUT_MS,
		pluginService:                 pluginService,
	}
	queryForTopologyFunc := func(conn driver.Conn) ([]*host_info_util.HostInfo, error) {
		monitor := m.getMonitor()
		return monitor.ForceRefreshUsingConn(conn, m.defaultTopologyQueryTimeoutMs)
	}
	clusterIdChangedFunc := func(oldClusterId string) {
		monitor, _ := clusterTopologyMonitors.Get(oldClusterId, MONITOR_EXPIRATION_NANOS)
		if monitor != nil {
			computeFunc := func() ClusterTopologyMonitor {
				return monitor
			}
			clusterTopologyMonitors.ComputeIfAbsent(m.clusterId, computeFunc, MONITOR_EXPIRATION_NANOS)
			monitor.SetClusterId(m.clusterId)
			clusterTopologyMonitors.Remove(oldClusterId)
		}

		existingHosts, ok := TopologyCache.Get(oldClusterId)
		if ok && existingHosts != nil {
			TopologyCache.Put(m.clusterId, existingHosts, TOPOLOGY_CACHE_EXPIRATION_NANO)
		}
	}
	m.RdsHostListProvider = NewRdsHostListProvider(hostListProviderService, databaseDialect, properties, originalDsn, queryForTopologyFunc, clusterIdChangedFunc)
	return m
}

func (m *MonitoringRdsHostListProvider) getMonitor() ClusterTopologyMonitor {
	monitor, ok := clusterTopologyMonitors.Get(m.clusterId, MONITOR_EXPIRATION_NANOS)
	if ok {
		return monitor
	}

	highRefreshRateNano := time.Millisecond * time.Duration(property_util.GetVerifiedWrapperPropertyValue[int](m.properties, property_util.CLUSTER_TOPOLOGY_HIGH_REFRESH_RATE_MS))

	computeFunc := func() ClusterTopologyMonitor {
		monitor = NewClusterTopologyMonitorImpl(
			m,
			m.databaseDialect,
			m.clusterId,
			highRefreshRateNano,
			m.refreshRateNanos,
			TOPOLOGY_CACHE_EXPIRATION_NANO,
			m.properties,
			m.initialHostInfo,
			m.clusterInstanceTemplate,
			m.pluginService)
		monitor.Start(clusterTopologyMonitorWg)
		clusterTopologyMonitorWg.Add(1)
		return monitor
	}
	return clusterTopologyMonitors.ComputeIfAbsent(m.clusterId, computeFunc, MONITOR_EXPIRATION_NANOS)
}

func (m *MonitoringRdsHostListProvider) ForceRefreshHostListWithTimeout(shouldVerifyWriter bool, timeoutMs int) ([]*host_info_util.HostInfo, error) {
	monitor := m.getMonitor()
	updatedHosts, err := monitor.ForceRefreshVerifyWriter(shouldVerifyWriter, timeoutMs)
	if err == nil && len(updatedHosts) > 0 {
		TopologyCache.Put(m.clusterId, updatedHosts, TOPOLOGY_CACHE_EXPIRATION_NANO)
	}
	return updatedHosts, err
}
