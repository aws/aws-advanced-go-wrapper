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
	"math"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

// Monitor-related constants
var (
	TOPOLOGY_CACHE_EXPIRATION_NANO    = time.Minute * 5
	DEFAULT_TOPOLOGY_QUERY_TIMEOUT_MS = 5000
)

type RdsHostListProvider struct {
	hostListProviderService HostListProviderService
	servicesContainer       ServicesContainer
	topologyUtils           TopologyUtils
	properties              *utils.RWMap[string, string]
	isInitialized           bool
	// Monitor-related fields
	pluginService                 PluginService
	defaultTopologyQueryTimeoutMs int
	// The following properties are initialized from the above in init().
	initialHostList         []*host_info_util.HostInfo
	initialHostInfo         *host_info_util.HostInfo
	clusterId               string
	clusterInstanceTemplate *host_info_util.HostInfo
	refreshRateNanos        time.Duration
	lock                    sync.Mutex
}

func NewRdsHostListProvider(
	hostListProviderService HostListProviderService,
	topologyUtils TopologyUtils,
	properties *utils.RWMap[string, string],
	servicesContainer ServicesContainer,
) *RdsHostListProvider {
	return &RdsHostListProvider{
		hostListProviderService:       hostListProviderService,
		topologyUtils:                 topologyUtils,
		properties:                    properties,
		servicesContainer:             servicesContainer,
		pluginService:                 servicesContainer.GetPluginService(),
		defaultTopologyQueryTimeoutMs: DEFAULT_TOPOLOGY_QUERY_TIMEOUT_MS,
		isInitialized:                 false,
	}
}

func (r *RdsHostListProvider) init() {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.isInitialized {
		return
	}
	refreshRateInt := property_util.GetRefreshRateValue(r.properties, property_util.CLUSTER_TOPOLOGY_REFRESH_RATE_MS)
	r.refreshRateNanos = time.Millisecond * time.Duration(refreshRateInt)
	hostListFromDsn, err := property_util.GetHostsFromProps(r.properties, false)
	if err != nil || len(hostListFromDsn) == 0 {
		return
	}
	r.initialHostList = hostListFromDsn
	r.initialHostInfo = r.initialHostList[0]
	r.hostListProviderService.SetInitialConnectionHostInfo(r.initialHostInfo)

	clusterInstancePattern := property_util.GetVerifiedWrapperPropertyValue[string](r.properties, property_util.CLUSTER_INSTANCE_HOST_PATTERN)
	defaultTemplate, errBuildingDefaultTemplate := (host_info_util.NewHostInfoBuilder()).SetHost(utils.GetRdsInstanceHostPattern(r.initialHostInfo.Host)).
		SetPort(r.initialHostInfo.Port).SetHostId(r.initialHostInfo.HostId).Build()
	if errBuildingDefaultTemplate != nil {
		// Should never be called. Host is explicitly set when building default template.
		return
	}

	if clusterInstancePattern != "" {
		r.clusterInstanceTemplate, err = property_util.ParseHostPortPair(clusterInstancePattern, r.initialHostInfo.Port)
	}
	if err == nil && !r.clusterInstanceTemplate.IsNil() {
		rdsUrlType := utils.IdentifyRdsUrlType(r.clusterInstanceTemplate.Host)

		if rdsUrlType == utils.RDS_PROXY || rdsUrlType == utils.RDS_CUSTOM_CLUSTER || !strings.Contains(r.clusterInstanceTemplate.Host, "?") {
			// Host can not be used as instance pattern.
			slog.Warn(error_util.GetMessage("RdsHostListProvider.givenTemplateInvalid"))
			r.clusterInstanceTemplate = defaultTemplate
		}
	} else {
		r.clusterInstanceTemplate = defaultTemplate
	}
	r.clusterId = property_util.GetVerifiedWrapperPropertyValue[string](r.properties, property_util.CLUSTER_ID)

	r.isInitialized = true
}

func (r *RdsHostListProvider) getOrCreateMonitor() ClusterTopologyMonitor {
	r.init()
	highRefreshRateNano := time.Millisecond * time.Duration(property_util.GetRefreshRateValue(r.properties, property_util.CLUSTER_TOPOLOGY_HIGH_REFRESH_RATE_MS))

	initializer := func(container ServicesContainer) (Monitor, error) {
		monitor := NewClusterTopologyMonitorImpl(
			container,
			r.topologyUtils,
			r.clusterId,
			highRefreshRateNano,
			r.refreshRateNanos,
			TOPOLOGY_CACHE_EXPIRATION_NANO,
			r.properties,
			r.initialHostInfo,
			r.clusterInstanceTemplate,
			r.pluginService)
		return monitor, nil
	}

	monitor, err := r.servicesContainer.GetMonitorService().RunIfAbsent(
		ClusterTopologyMonitorType,
		r.clusterId,
		r.servicesContainer,
		initializer)
	if err != nil {
		return nil
	}
	return monitor.(ClusterTopologyMonitor)
}

// =============================================================================
// HostListProvider Interface Implementation
// =============================================================================

func (r *RdsHostListProvider) ForceRefresh() ([]*host_info_util.HostInfo, error) {
	return r.ForceRefreshWithOptions(false, DEFAULT_TOPOLOGY_QUERY_TIMEOUT_MS)
}

func (r *RdsHostListProvider) ForceRefreshWithOptions(verifyTopology bool, timemoutMs int) ([]*host_info_util.HostInfo, error) {
	return r.forceRefreshMonitor(verifyTopology, timemoutMs)
}

func (r *RdsHostListProvider) forceRefreshMonitor(verifyTopology bool, timemoutMs int) ([]*host_info_util.HostInfo, error) {
	r.init()
	if !r.pluginService.IsDialectConfirmed() {
		return r.initialHostList, nil
	}
	monitor := r.getOrCreateMonitor()
	return monitor.ForceRefresh(verifyTopology, timemoutMs)
}

func (r *RdsHostListProvider) GetClusterId() (string, error) {
	r.init()
	return r.clusterId, nil
}

func (r *RdsHostListProvider) GetHostRole(conn driver.Conn) host_info_util.HostRole {
	return r.topologyUtils.GetHostRole(conn)
}

func (r *RdsHostListProvider) IdentifyConnection(conn driver.Conn) (*host_info_util.HostInfo, error) {
	r.init()
	_, instanceName := r.topologyUtils.GetInstanceId(conn)
	if instanceName != "" {
		topology, err := r.Refresh()
		forcedRefresh := false
		if err != nil || len(topology) == 0 {
			topology, err = r.ForceRefresh()
			forcedRefresh = true
		}
		if err != nil {
			return nil, err
		}
		if len(topology) == 0 {
			return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("RdsHostListProvider.unableToGatherTopology"))
		}
		foundHost := utils.FindHostInTopology(topology, instanceName, r.getHostEndpoint(instanceName))

		if foundHost.IsNil() && !forcedRefresh {
			topology, err = r.ForceRefresh()
			if err != nil {
				return nil, err
			}
			foundHost = utils.FindHostInTopology(topology, instanceName, r.getHostEndpoint(instanceName))
		}
		if !foundHost.IsNil() {
			return foundHost, nil
		}
	}
	return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("RdsHostListProvider.unableToGetHostName"))
}

func (r *RdsHostListProvider) IsDynamicHostListProvider() bool {
	return true
}

func (r *RdsHostListProvider) Refresh() ([]*host_info_util.HostInfo, error) {
	r.init()
	hosts, isCachedData, err := r.getTopology()
	if err != nil {
		return nil, err
	}

	msgPrefix := "From SQL Query"
	if isCachedData {
		msgPrefix = "From cache"
	}
	slog.Info(utils.LogTopology(hosts, msgPrefix))
	return host_info_util.CopyHostList(hosts), nil
}

func (r *RdsHostListProvider) CreateHost(host string, hostRole host_info_util.HostRole, lag float64, cpu float64, lastUpdateTime time.Time) *host_info_util.HostInfo {
	builder := host_info_util.NewHostInfoBuilder()

	weight := int(math.Round(lag)*100 + math.Round(cpu))
	var port int
	if r.clusterInstanceTemplate.Port != host_info_util.HOST_NO_PORT {
		port = r.clusterInstanceTemplate.Port
	} else {
		if r.initialHostInfo.Port != host_info_util.HOST_NO_PORT {
			port = r.initialHostInfo.Port
		} else {
			port = r.hostListProviderService.GetDialect().GetDefaultPort()
		}
	}

	host = r.getHostEndpoint(host)

	builder.SetHost(host).SetPort(port).SetRole(hostRole).SetAvailability(host_info_util.AVAILABLE).SetWeight(weight).SetLastUpdateTime(lastUpdateTime)

	hostInfo, _ := builder.Build()
	return hostInfo
}

func (r *RdsHostListProvider) StopMonitor() {
	r.servicesContainer.GetMonitorService().StopAndRemove(ClusterTopologyMonitorType, r.clusterId)
}

// =============================================================================
// BlockingHostListProvider Interface Implementation
// =============================================================================

func (r *RdsHostListProvider) ForceRefreshHostListWithTimeout(shouldVerifyWriter bool, timeoutMs int) ([]*host_info_util.HostInfo, error) {
	monitor := r.getOrCreateMonitor()
	updatedHosts, err := monitor.ForceRefresh(shouldVerifyWriter, timeoutMs)
	if err == nil && len(updatedHosts) > 0 {
		TopologyStorageType.Set(r.servicesContainer.GetStorageService(), r.clusterId, NewTopology(updatedHosts))
	}
	return updatedHosts, err
}

// =============================================================================
// Internal Helpers
// =============================================================================

func (r *RdsHostListProvider) getTopology() ([]*host_info_util.HostInfo, bool, error) {
	hosts := r.getStoredHosts()

	if !r.pluginService.IsDialectConfirmed() {
		// We need to confirm the dialect before creating a topology monitor so that it uses the correct SQL queries.
		// We will return the original hosts parsed from the connection string until the dialect has been confirmed.
		return r.initialHostList, false, nil
	}

	if len(hosts) == 0 {
		// We need to re-fetch topology - start the monitor and get topology
		refreshedHosts, err := r.forceRefreshMonitor(false, DEFAULT_TOPOLOGY_QUERY_TIMEOUT_MS)
		if err == nil && len(refreshedHosts) > 0 {
			return refreshedHosts, false, nil
		}
	}

	if len(hosts) > 0 {
		// Return the cached hosts.
		return hosts, true, nil
	}
	if len(r.initialHostList) > 0 {
		// Return the initial hosts.
		return r.initialHostList, false, nil
	}

	return nil, false, error_util.NewGenericAwsWrapperError(error_util.GetMessage("RdsHostListProvider.unableToGatherTopology"))
}

func (r *RdsHostListProvider) getStoredHosts() []*host_info_util.HostInfo {
	r.init()
	topology, found := TopologyStorageType.Get(r.servicesContainer.GetStorageService(), r.clusterId)
	if !found {
		return nil
	}
	return topology.GetHosts()
}

func (r *RdsHostListProvider) getHostEndpoint(hostName string) string {
	host := r.clusterInstanceTemplate.Host
	return strings.ReplaceAll(host, "?", hostName)
}

// =============================================================================
// Cache Management
// =============================================================================

// ClearAllRdsHostListProviderCaches clears all cluster topology monitors.
// Note: This requires access to a ServicesContainer to clear monitors via MonitorService.
// For global cleanup, use MonitorService.StopAndRemoveByType(ClusterTopologyMonitorType) directly.
func ClearAllRdsHostListProviderCaches(monitorService MonitorService) {
	if monitorService != nil {
		monitorService.StopAndRemoveByType(ClusterTopologyMonitorType)
	}
}
