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
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"

	"github.com/google/uuid"
)

func NewRdsHostListProvider(
	hostListProviderService HostListProviderService,
	databaseDialect TopologyAwareDialect,
	properties *utils.RWMap[string, string],
	queryForTopologyFunc func(conn driver.Conn) ([]*host_info_util.HostInfo, error),
	clusterIdChangedFunc func(oldClusterId string)) *RdsHostListProvider {
	r := &RdsHostListProvider{
		hostListProviderService: hostListProviderService,
		databaseDialect:         databaseDialect,
		properties:              properties,
		isInitialized:           false,
	}
	if queryForTopologyFunc == nil {
		queryForTopologyFunc = func(conn driver.Conn) ([]*host_info_util.HostInfo, error) {
			return r.databaseDialect.GetTopology(conn, r)
		}
	}
	r.queryForTopologyFunc = queryForTopologyFunc
	r.clusterIdChangedFunc = clusterIdChangedFunc
	return r
}

var primaryClusterIdCache = utils.NewCache[bool]()
var suggestedPrimaryClusterCache = utils.NewCache[string]()
var TopologyCache = utils.NewCache[[]*host_info_util.HostInfo]()

type RdsHostListProvider struct {
	hostListProviderService HostListProviderService
	databaseDialect         TopologyAwareDialect
	properties              *utils.RWMap[string, string]
	isInitialized           bool
	// The following properties are initialized from the above in init().
	initialHostList         []*host_info_util.HostInfo
	initialHostInfo         *host_info_util.HostInfo
	IsPrimaryClusterId      bool
	clusterId               string
	clusterInstanceTemplate *host_info_util.HostInfo
	refreshRateNanos        time.Duration
	lock                    sync.Mutex
	queryForTopologyFunc    func(driver.Conn) ([]*host_info_util.HostInfo, error)
	clusterIdChangedFunc    func(oldClusterId string)
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

	r.clusterId = uuid.New().String()
	r.IsPrimaryClusterId = false
	rdsUrlType := utils.IdentifyRdsUrlType(r.initialHostInfo.Host)
	clusterIdSetting := property_util.GetVerifiedWrapperPropertyValue[string](r.properties, property_util.CLUSTER_ID)

	if clusterIdSetting != "" {
		r.clusterId = clusterIdSetting
	} else if rdsUrlType == utils.RDS_PROXY {
		r.clusterId = r.initialHostInfo.GetUrl()
	} else if rdsUrlType.IsRds {
		suggestedClusterId, isPrimary := r.getSuggestedClusterId(r.initialHostInfo.GetHostAndPort())
		if suggestedClusterId != "" {
			r.clusterId = suggestedClusterId
			r.IsPrimaryClusterId = isPrimary
		} else {
			clusterRdsHostUrl := utils.GetRdsClusterHostUrl(r.initialHostInfo.Host)
			if clusterRdsHostUrl != "" {
				if r.clusterInstanceTemplate.Port != 0 {
					r.clusterId = fmt.Sprintf("%s:%d", clusterRdsHostUrl, r.clusterInstanceTemplate.Port)
				} else {
					r.clusterId = clusterRdsHostUrl
				}
				r.IsPrimaryClusterId = true
				primaryClusterIdCache.Put(r.clusterId, true, utils.CleanupIntervalNanos)
			}
		}
	}

	r.isInitialized = true
}

func (r *RdsHostListProvider) ForceRefresh(conn driver.Conn) ([]*host_info_util.HostInfo, error) {
	r.init()
	if conn == nil {
		conn = r.hostListProviderService.GetCurrentConnection()
	}
	hosts, _, err := r.getTopology(conn, true)
	if err != nil {
		return nil, err
	}
	slog.Debug(utils.LogTopology(hosts, "From ForceRefresh"))
	return hosts, nil
}

func (r *RdsHostListProvider) GetClusterId() (string, error) {
	r.init()
	return r.clusterId, nil
}

func (r *RdsHostListProvider) GetHostRole(conn driver.Conn) host_info_util.HostRole {
	return r.databaseDialect.GetHostRole(conn)
}

func (r *RdsHostListProvider) IdentifyConnection(conn driver.Conn) (*host_info_util.HostInfo, error) {
	instanceName := r.databaseDialect.GetHostName(conn)
	if instanceName != "" {
		topology, err := r.Refresh(conn)
		forcedRefresh := false
		if err != nil || len(topology) == 0 {
			topology, err = r.ForceRefresh(conn)
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
			topology, err = r.ForceRefresh(conn)
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

func (r *RdsHostListProvider) IsStaticHostListProvider() bool {
	return false
}

func (r *RdsHostListProvider) Refresh(conn driver.Conn) ([]*host_info_util.HostInfo, error) {
	r.init()
	if conn == nil {
		conn = r.hostListProviderService.GetCurrentConnection()
	}
	hosts, isCachedData, err := r.getTopology(conn, false)
	if err != nil {
		return nil, err
	}
	msgPrefix := "From SQL Query"
	if isCachedData {
		msgPrefix = "From cache"
	}
	slog.Info(utils.LogTopology(hosts, msgPrefix))
	return hosts, nil
}

func (r *RdsHostListProvider) getTopology(conn driver.Conn, forceUpdate bool) ([]*host_info_util.HostInfo, bool, error) {
	r.lock.Lock()
	suggestedPrimaryId, ok := suggestedPrimaryClusterCache.Get(r.clusterId)
	if ok && suggestedPrimaryId != "" && r.clusterId != suggestedPrimaryId {
		oldClusterId := r.clusterId
		r.clusterId = suggestedPrimaryId
		r.IsPrimaryClusterId = true
		r.clusterIdChangedFunc(oldClusterId)
	}
	r.lock.Unlock()

	hosts, ok := TopologyCache.Get(r.clusterId)

	// If this cluster id is a primary one, and about to create a new entry in the cache
	// it needs to be suggested for other non-primary clusters.
	needToSuggest := (!ok || len(hosts) == 0) && r.IsPrimaryClusterId

	if (!ok || forceUpdate || len(hosts) == 0) && conn != nil {
		// Need to fetch the topology.
		hosts, err := r.queryForTopologyFunc(conn)
		if err != nil {
			// Topology fetch failed, pass on error.
			return nil, false, err
		}
		if len(hosts) > 0 {
			TopologyCache.Put(r.clusterId, hosts, r.refreshRateNanos)
			if needToSuggest {
				r.suggestPrimaryCluster(hosts)
			}
			return hosts, false, nil
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

func (r *RdsHostListProvider) getSuggestedClusterId(url string) (string, bool) {
	for key, hosts := range TopologyCache.GetAllEntries() {
		isPrimaryCluster, ok := primaryClusterIdCache.Get(key)
		if ok && isPrimaryCluster && key == url {
			return url, isPrimaryCluster
		}
		if len(hosts) == 0 || !ok {
			continue
		}
		for _, host := range hosts {
			if host.GetHostAndPort() == url {
				slog.Info(error_util.GetMessage("RdsHostListProvider.suggestedClusterId", key, url))
				return key, isPrimaryCluster
			}
		}
	}
	return "", false
}

func (r *RdsHostListProvider) suggestPrimaryCluster(primaryClusterHosts []*host_info_util.HostInfo) {
	if len(primaryClusterHosts) == 0 {
		return
	}

	primaryClusterHostUrls := map[string]bool{}
	for _, hostInfo := range primaryClusterHosts {
		primaryClusterHostUrls[hostInfo.GetUrl()] = true
	}

	for clusterId, clusterHosts := range TopologyCache.GetAllEntries() {
		isPrimaryCluster, ok := primaryClusterIdCache.Get(clusterId)
		suggestedPrimaryClusterId, ok2 := suggestedPrimaryClusterCache.Get(clusterId)
		// No further action if the cluster is primary, there is no suggestion, or there are no hosts.
		if (ok && isPrimaryCluster) || (!ok2 || suggestedPrimaryClusterId == "") || len(clusterHosts) == 0 {
			continue
		}

		// The entry is not primary.
		for _, host := range clusterHosts {
			if primaryClusterHostUrls[host.GetUrl()] {
				// Instance in this cluster matches one instance on primary cluster. Suggest primary cluster id.
				suggestedPrimaryClusterCache.Put(clusterId, r.clusterId, utils.CleanupIntervalNanos)
				break
			}
		}
	}
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

func (r *RdsHostListProvider) getHostEndpoint(hostName string) string {
	host := r.clusterInstanceTemplate.Host
	return strings.Replace(host, "?", hostName, -1)
}

func (r *RdsHostListProvider) StopMonitor() {
	// do nothing since this provider doesn't implement active monitoring
}

func ClearAllRdsHostListProviderCaches() {
	TopologyCache.Clear()
	primaryClusterIdCache.Clear()
	suggestedPrimaryClusterCache.Clear()
}
