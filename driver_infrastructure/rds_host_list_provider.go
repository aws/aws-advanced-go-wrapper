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
	"awssql/error_util"
	"awssql/host_info_util"
	"awssql/property_util"
	"awssql/utils"
	"database/sql/driver"
	"log/slog"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

func NewRdsHostListProvider(hostListProviderService HostListProviderService, databaseDialect TopologyAwareDialect, properties map[string]string,
	originalDsn string) *RdsHostListProvider {
	return &RdsHostListProvider{
		hostListProviderService: hostListProviderService,
		databaseDialect:         databaseDialect,
		properties:              properties,
		originalDsn:             originalDsn,
		isInitialized:           false,
	}
}

var primaryClusterIdCache = utils.NewCache[bool]()
var suggestedPrimaryClusterCache = utils.NewCache[string]()
var TopologyCache = utils.NewCache[[]host_info_util.HostInfo]()

type RdsHostListProvider struct {
	hostListProviderService HostListProviderService
	databaseDialect         TopologyAwareDialect
	properties              map[string]string
	originalDsn             string
	isInitialized           bool
	// The following properties are initialized from the above in init().
	initialHostList         []host_info_util.HostInfo
	initialHostInfo         host_info_util.HostInfo
	IsPrimaryClusterId      bool
	clusterId               string
	clusterInstanceTemplate host_info_util.HostInfo
	refreshRateNanos        time.Duration
	lock                    sync.Mutex
}

func (r *RdsHostListProvider) init() {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.isInitialized {
		return
	}
	r.refreshRateNanos = time.Millisecond * time.Duration(property_util.GetVerifiedWrapperPropertyValue[int](r.properties, property_util.CLUSTER_TOPOLOGY_REFRESH_RATE_MS))
	hostListFromDsn, err := utils.GetHostsFromDsn(r.originalDsn, false)
	if err != nil || len(hostListFromDsn) == 0 {
		return
	}
	r.initialHostList = hostListFromDsn
	r.initialHostInfo = r.initialHostList[0]

	clusterInstancePattern := property_util.GetVerifiedWrapperPropertyValue[string](r.properties, property_util.CLUSTER_INSTANCE_HOST_PATTERN)
	defaultTemplate := (host_info_util.NewHostInfoBuilder()).SetHost(utils.GetRdsInstanceHostPattern(r.initialHostInfo.Host)).
		SetPort(r.initialHostInfo.Port).SetHostId(r.initialHostInfo.HostId).Build()
	if clusterInstancePattern != "" {
		r.clusterInstanceTemplate, err = utils.ParseHostPortPair(clusterInstancePattern)
	}
	if err == nil {
		rdsUrlType := utils.IdentifyRdsUrlType(r.clusterInstanceTemplate.Host)
		if rdsUrlType == utils.RDS_PROXY || rdsUrlType == utils.RDS_CUSTOM_CLUSTER || !strings.Contains(r.clusterInstanceTemplate.Host, "?") {
			// Host can not be used as instance pattern.
			slog.Warn(error_util.GetMessage("RdsHostListProvider.givenTemplateInvalid"))
			r.clusterInstanceTemplate = *defaultTemplate
		}
	} else {
		r.clusterInstanceTemplate = *defaultTemplate
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
			r.clusterId = r.originalDsn
			r.IsPrimaryClusterId = true
			primaryClusterIdCache.Put(r.clusterId, true, utils.CleanupIntervalNanos)
		}
	}

	r.isInitialized = true
}

func (r *RdsHostListProvider) ForceRefresh(conn driver.Conn) (hosts []host_info_util.HostInfo) {
	r.init()
	if conn == nil {
		conn = *r.hostListProviderService.GetCurrentConnection()
	}
	hosts, _ = r.getTopology(conn, true)
	utils.LogTopology(hosts, "From ForceRefresh")
	return
}

func (r *RdsHostListProvider) GetClusterId() string {
	r.init()
	return r.clusterId
}

func (r *RdsHostListProvider) GetHostRole(conn driver.Conn) host_info_util.HostRole {
	return r.databaseDialect.GetHostRole(conn)
}

func (r *RdsHostListProvider) IdentifyConnection(conn driver.Conn) host_info_util.HostInfo {
	instanceName := r.databaseDialect.GetHostName(conn)
	if instanceName != "" {
		topology := r.Refresh(conn)
		forcedRefresh := false
		if len(topology) == 0 {
			topology = r.ForceRefresh(conn)
			forcedRefresh = true
		}
		if len(topology) == 0 {
			return host_info_util.HostInfo{}
		}
		foundHost := utils.FindHostInTopology(topology, instanceName)

		if foundHost.Host == "" && !forcedRefresh {
			topology = r.ForceRefresh(conn)
			foundHost = utils.FindHostInTopology(topology, instanceName)
		}
		return foundHost
	}
	return host_info_util.HostInfo{}
}

func (r *RdsHostListProvider) IsStaticHostListProvider() bool {
	return false
}

func (r *RdsHostListProvider) Refresh(conn driver.Conn) (hosts []host_info_util.HostInfo) {
	r.init()
	if conn == nil {
		conn = *r.hostListProviderService.GetCurrentConnection()
	}
	hosts, isCachedData := r.getTopology(conn, false)
	msgPrefix := "From SQL Query"
	if isCachedData {
		msgPrefix = "From cache"
	}
	utils.LogTopology(hosts, msgPrefix)
	return
}

func (r *RdsHostListProvider) getTopology(conn driver.Conn, forceUpdate bool) (hosts []host_info_util.HostInfo, isCachedData bool) {
	r.lock.Lock()
	suggestedPrimaryId, ok := suggestedPrimaryClusterCache.Get(r.clusterId)
	if ok && suggestedPrimaryId != "" && r.clusterId != suggestedPrimaryId {
		r.clusterId = suggestedPrimaryId
		r.IsPrimaryClusterId = true
	}
	r.lock.Unlock()

	hosts, ok = TopologyCache.Get(r.clusterId)

	// If this cluster id is a primary one, and about to create a new entry in the cache
	// it needs to be suggested for other non-primary clusters.
	needToSuggest := (!ok || len(hosts) == 0) && r.IsPrimaryClusterId

	if (!ok || forceUpdate || len(hosts) == 0) && conn != nil {
		// Need to fetch the topology.
		hosts = r.databaseDialect.GetTopology(conn, r)
		if len(hosts) > 0 {
			TopologyCache.Put(r.clusterId, hosts, r.refreshRateNanos)
			if needToSuggest {
				r.suggestPrimaryCluster(hosts)
			}
			return hosts, false
		}
	}
	if len(hosts) > 0 {
		// Return the cached hosts.
		return hosts, true
	}
	// Return the initial hosts.
	return r.initialHostList, false
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

func (r *RdsHostListProvider) suggestPrimaryCluster(primaryClusterHosts []host_info_util.HostInfo) {
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

func (r *RdsHostListProvider) createHost(host string, hostRole host_info_util.HostRole, lag float64, cpu float64, lastUpdateTime time.Time) host_info_util.HostInfo {
	builder := host_info_util.NewHostInfoBuilder()

	weight := int(math.Round(lag)*100 + math.Round(cpu))
	var port int
	if r.clusterInstanceTemplate.Port != host_info_util.HOST_NO_PORT {
		port = r.clusterInstanceTemplate.Port
	} else {
		port = r.initialHostInfo.Port
	}
	host = r.getHostEndpoint(host)

	builder.SetHost(host).SetPort(port).SetRole(hostRole).SetAvailability(host_info_util.AVAILABLE).SetWeight(weight).SetLastUpdateTime(lastUpdateTime)

	return *builder.Build()
}

func (r *RdsHostListProvider) getHostEndpoint(hostName string) string {
	host := r.clusterInstanceTemplate.Host
	return strings.Replace(host, "?", hostName, -1)
}

func ClearAllRdsHostListProviderCaches() {
	TopologyCache.Clear()
	primaryClusterIdCache.Clear()
	suggestedPrimaryClusterCache.Clear()
}
