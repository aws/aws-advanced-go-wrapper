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
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

// Monitor-related constants.
var (
	TOPOLOGY_CACHE_EXPIRATION_NANO    = time.Minute * 5
	DEFAULT_TOPOLOGY_QUERY_TIMEOUT_MS = 5000
)

type RdsHostListProvider struct {
	hostListProviderService       HostListProviderService
	servicesContainer             ServicesContainer
	topologyUtils                 TopologyUtils
	properties                    *utils.RWMap[string, string]
	isInitialized                 bool
	defaultTopologyQueryTimeoutMs int
	monitorCreator                func() (ClusterTopologyMonitor, error)
	initialHostList               []*host_info_util.HostInfo
	initialHostInfo               *host_info_util.HostInfo
	clusterId                     string
	clusterInstanceTemplate       *host_info_util.HostInfo
	refreshRateNanos              time.Duration
	lock                          sync.Mutex
}

func NewRdsHostListProvider(
	hostListProviderService HostListProviderService,
	clusterTopologyUtils ClusterTopologyUtils,
	properties *utils.RWMap[string, string],
	servicesContainer ServicesContainer,
	monitorCreator func() (ClusterTopologyMonitor, error),
) *RdsHostListProvider {
	r := &RdsHostListProvider{
		hostListProviderService:       hostListProviderService,
		topologyUtils:                 clusterTopologyUtils,
		properties:                    properties,
		servicesContainer:             servicesContainer,
		defaultTopologyQueryTimeoutMs: DEFAULT_TOPOLOGY_QUERY_TIMEOUT_MS,
		isInitialized:                 false,
	}
	if monitorCreator != nil {
		r.monitorCreator = monitorCreator
	} else {
		r.monitorCreator = func() (ClusterTopologyMonitor, error) {
			r.init()
			return createClusterTopologyMonitor(
				r.servicesContainer, r.clusterId, r.properties,
				r.initialHostInfo, r.clusterInstanceTemplate, r.refreshRateNanos,
				&rdsTopologyQueryStrategy{
					topologyUtils:           clusterTopologyUtils,
					initialHostInfo:         r.initialHostInfo,
					clusterInstanceTemplate: r.clusterInstanceTemplate,
				})
		}
	}
	return r
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

func getMonitoringProps(props *utils.RWMap[string, string], resolver DriverPropertyResolver) *utils.RWMap[string, string] {
	monitoringProps := utils.NewRWMapFromMap(props.GetAllEntries())

	connectTimeoutMs := property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.CLUSTER_TOPOLOGY_CONNECT_TIMEOUT_MS)
	socketTimeoutMs := property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.CLUSTER_TOPOLOGY_SOCKET_TIMEOUT_MS)

	driverProps := resolver.CreateProps(
		WithProperty(ConnectTimeout, connectTimeoutMs),
		WithProperty(SocketTimeout, socketTimeoutMs),
	)
	for k, v := range driverProps {
		monitoringProps.Put(k, v)
	}

	return monitoringProps
}

func createClusterTopologyMonitor(
	servicesContainer ServicesContainer,
	clusterId string,
	props *utils.RWMap[string, string],
	initialHostInfo *host_info_util.HostInfo,
	clusterInstanceTemplate *host_info_util.HostInfo,
	refreshRateNanos time.Duration,
	strategy TopologyQueryStrategy,
) (ClusterTopologyMonitor, error) {
	highRefreshRateNano := time.Millisecond * time.Duration(
		property_util.GetRefreshRateValue(props, property_util.CLUSTER_TOPOLOGY_HIGH_REFRESH_RATE_MS))
	monitoringProps := getMonitoringProps(props, servicesContainer.GetPluginService().GetTargetDriverDialect().GetPropertyResolver())

	initializer := func(container ServicesContainer) (Monitor, error) {
		monitor := NewClusterTopologyMonitorImpl(
			container,
			clusterId,
			highRefreshRateNano,
			refreshRateNanos,
			TOPOLOGY_CACHE_EXPIRATION_NANO,
			monitoringProps,
			initialHostInfo,
			clusterInstanceTemplate,
			servicesContainer.GetPluginService(),
			strategy)
		return monitor, nil
	}

	monitor, err := servicesContainer.GetMonitorService().RunIfAbsent(
		ClusterTopologyMonitorType, clusterId, servicesContainer, initializer)
	if err != nil {
		return nil, err
	}
	return monitor.(ClusterTopologyMonitor), nil
}

// =============================================================================
// HostListProvider Interface Implementation
// =============================================================================

func (r *RdsHostListProvider) ForceRefresh() ([]*host_info_util.HostInfo, error) {
	return r.forceRefreshMonitor(false, DEFAULT_TOPOLOGY_QUERY_TIMEOUT_MS)
}

func (r *RdsHostListProvider) forceRefreshMonitor(verifyTopology bool, timeoutMs int) ([]*host_info_util.HostInfo, error) {
	r.init()
	if !r.servicesContainer.GetPluginService().IsDialectConfirmed() {
		return r.initialHostList, nil
	}
	monitor, err := r.monitorCreator()
	if err != nil {
		return nil, err
	}
	return monitor.ForceRefresh(verifyTopology, timeoutMs)
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
	instanceId, instanceName := r.topologyUtils.GetInstanceId(conn)
	if instanceName == "" {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("RdsHostListProvider.unableToGetHostName"))
	}

	topology, err := r.Refresh()
	if err != nil {
		return nil, err
	}
	forcedRefresh := false
	if len(topology) == 0 {
		topology, err = r.ForceRefresh()
		forcedRefresh = true
	}
	if err != nil {
		return nil, err
	}
	if len(topology) == 0 {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("RdsHostListProvider.unableToGatherTopology"))
	}

	foundHost := findHost(topology, instanceId, instanceName)

	if foundHost.IsNil() && !forcedRefresh {
		topology, err = r.ForceRefresh()
		if err != nil {
			return nil, err
		}
		foundHost = findHost(topology, instanceId, instanceName)
	}
	if !foundHost.IsNil() {
		return foundHost, nil
	}

	return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("RdsHostListProvider.unableToGetHostName"))
}

func findHost(hosts []*host_info_util.HostInfo, instanceId string, instanceName string) *host_info_util.HostInfo {
	for _, host := range hosts {
		if instanceId == host.HostId || instanceName == host.HostId || instanceName == host.Host {
			return host
		}
	}
	return nil
}

func (r *RdsHostListProvider) IsStaticHostListProvider() bool {
	return false
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

func (r *RdsHostListProvider) StopMonitor() {
	r.servicesContainer.GetMonitorService().StopAndRemove(ClusterTopologyMonitorType, r.clusterId)
}

func (r *RdsHostListProvider) ForceRefreshHostListWithTimeout(shouldVerifyWriter bool, timeoutMs int) ([]*host_info_util.HostInfo, error) {
	monitor, err := r.monitorCreator()
	if err != nil {
		return nil, err
	}
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

	if !r.servicesContainer.GetPluginService().IsDialectConfirmed() {
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

// =============================================================================
// RDS Topology Query Strategy
// =============================================================================

// rdsTopologyQueryStrategy is the standard strategy for RDS clusters (Aurora and Multi-AZ).
type rdsTopologyQueryStrategy struct {
	topologyUtils           ClusterTopologyUtils
	initialHostInfo         *host_info_util.HostInfo
	clusterInstanceTemplate *host_info_util.HostInfo
}

func (d *rdsTopologyQueryStrategy) QueryForTopology(conn driver.Conn) ([]*host_info_util.HostInfo, error) {
	return d.topologyUtils.QueryForTopology(conn, d.initialHostInfo, d.clusterInstanceTemplate)
}

func (d *rdsTopologyQueryStrategy) GetInstanceTemplate(_ string, _ driver.Conn) (*host_info_util.HostInfo, error) {
	return nil, nil // nil signals: use the default clusterInstanceTemplate
}

func (d *rdsTopologyQueryStrategy) IsWriterInstance(conn driver.Conn) (bool, error) {
	return d.topologyUtils.IsWriterInstance(conn)
}

func (d *rdsTopologyQueryStrategy) GetInstanceId(conn driver.Conn) (string, string) {
	return d.topologyUtils.GetInstanceId(conn)
}

func (d *rdsTopologyQueryStrategy) CreateHost(
	instanceId, instanceName string, isWriter bool, weight int,
	lastUpdateTime time.Time, initialHost, instanceTemplate *host_info_util.HostInfo,
) *host_info_util.HostInfo {
	return d.topologyUtils.CreateHost(instanceId, instanceName, isWriter, weight, lastUpdateTime, initialHost, instanceTemplate)
}
