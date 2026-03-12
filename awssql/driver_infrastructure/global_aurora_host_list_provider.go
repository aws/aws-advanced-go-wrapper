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
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

type GlobalAuroraHostListProvider struct {
	base                      *RdsHostListProvider
	globalTopologyUtils       GlobalClusterTopologyUtils
	instanceTemplatesByRegion map[string]*host_info_util.HostInfo
	globalInitialized         bool
}

func NewGlobalAuroraHostListProvider(
	hostListProviderService HostListProviderService,
	topologyUtils GlobalClusterTopologyUtils,
	properties *utils.RWMap[string, string],
	servicesContainer ServicesContainer,
) (*GlobalAuroraHostListProvider, error) {
	instanceTemplatesStr := property_util.GLOBAL_CLUSTER_INSTANCE_HOST_PATTERNS.Get(properties)
	if instanceTemplatesStr == "" {
		return nil, error_util.NewGenericAwsWrapperError(
			error_util.GetMessage("GlobalAuroraTopologyUtils.globalClusterInstanceHostPatternsRequired"))
	}

	g := &GlobalAuroraHostListProvider{
		globalTopologyUtils: topologyUtils,
	}

	g.base = &RdsHostListProvider{
		hostListProviderService:       hostListProviderService,
		topologyUtils:                 topologyUtils,
		properties:                    properties,
		servicesContainer:             servicesContainer,
		defaultTopologyQueryTimeoutMs: DEFAULT_TOPOLOGY_QUERY_TIMEOUT_MS,
		isInitialized:                 false,
		// Closure captures the global topology utils and builds the global-aware strategy.
		monitorCreator: func() (ClusterTopologyMonitor, error) {
			if err := g.initGlobal(); err != nil {
				return nil, err
			}
			return createClusterTopologyMonitor(
				g.base.servicesContainer, g.base.clusterId, g.base.properties,
				g.base.initialHostInfo, g.base.clusterInstanceTemplate, g.base.refreshRateNanos,
				&globalAuroraTopologyQueryStrategy{
					topologyUtils:             topologyUtils,
					initialHostInfo:           g.base.initialHostInfo,
					clusterInstanceTemplate:   g.base.clusterInstanceTemplate,
					instanceTemplatesByRegion: g.instanceTemplatesByRegion,
				})
		},
	}

	return g, nil
}

func (g *GlobalAuroraHostListProvider) initGlobal() error {
	g.base.init()
	if g.globalInitialized {
		return nil
	}

	instanceTemplatesStr := property_util.GLOBAL_CLUSTER_INSTANCE_HOST_PATTERNS.Get(g.base.properties)
	defaultPort := g.base.servicesContainer.GetPluginService().GetDialect().GetDefaultPort()

	templates, err := ParseInstanceTemplates(instanceTemplatesStr, defaultPort)
	if err != nil {
		return err
	}
	g.instanceTemplatesByRegion = templates
	g.globalInitialized = true
	return nil
}

// =============================================================================
// HostListProvider interface — delegated to base with monitor override
// =============================================================================

func (g *GlobalAuroraHostListProvider) Refresh() ([]*host_info_util.HostInfo, error) {
	return g.base.Refresh()
}

func (g *GlobalAuroraHostListProvider) ForceRefresh() ([]*host_info_util.HostInfo, error) {
	return g.base.ForceRefresh()
}

func (g *GlobalAuroraHostListProvider) GetHostRole(conn driver.Conn) host_info_util.HostRole {
	return g.base.GetHostRole(conn)
}

func (g *GlobalAuroraHostListProvider) IdentifyConnection(conn driver.Conn) (*host_info_util.HostInfo, error) {
	return g.base.IdentifyConnection(conn)
}

func (g *GlobalAuroraHostListProvider) GetClusterId() (string, error) {
	return g.base.GetClusterId()
}

func (g *GlobalAuroraHostListProvider) StopMonitor() {
	g.base.StopMonitor()
}

func (g *GlobalAuroraHostListProvider) IsStaticHostListProvider() bool {
	return false
}

func (g *GlobalAuroraHostListProvider) ForceRefreshHostListWithTimeout(shouldVerifyWriter bool, timeoutMs int) ([]*host_info_util.HostInfo, error) {
	return g.base.ForceRefreshHostListWithTimeout(shouldVerifyWriter, timeoutMs)
}

// =============================================================================
// TopologyQueryStrategy interface
// =============================================================================
type globalAuroraTopologyQueryStrategy struct {
	topologyUtils             GlobalClusterTopologyUtils
	initialHostInfo           *host_info_util.HostInfo
	clusterInstanceTemplate   *host_info_util.HostInfo
	instanceTemplatesByRegion map[string]*host_info_util.HostInfo
}

func (s *globalAuroraTopologyQueryStrategy) QueryForTopology(conn driver.Conn) ([]*host_info_util.HostInfo, error) {
	return s.topologyUtils.QueryForTopologyByRegion(conn, s.initialHostInfo, s.instanceTemplatesByRegion)
}

func (s *globalAuroraTopologyQueryStrategy) GetInstanceTemplate(instanceId string, conn driver.Conn) (*host_info_util.HostInfo, error) {
	region, err := s.topologyUtils.GetRegion(instanceId, conn)
	if err != nil {
		return nil, err
	}
	if region != "" {
		template, found := s.instanceTemplatesByRegion[region]
		if !found {
			return nil, error_util.NewGenericAwsWrapperError(
				error_util.GetMessage("GlobalAuroraTopologyMonitor.cannotFindRegionTemplate", region))
		}
		return template, nil
	}
	// Region not found for this instance; fall back to default template.
	return nil, nil
}

func (s *globalAuroraTopologyQueryStrategy) IsWriterInstance(conn driver.Conn) (bool, error) {
	return s.topologyUtils.IsWriterInstance(conn)
}

func (s *globalAuroraTopologyQueryStrategy) GetInstanceId(conn driver.Conn) (string, string) {
	return s.topologyUtils.GetInstanceId(conn)
}

func (s *globalAuroraTopologyQueryStrategy) CreateHost(
	instanceId, instanceName string, isWriter bool, weight int,
	lastUpdateTime time.Time, initialHost, instanceTemplate *host_info_util.HostInfo,
) *host_info_util.HostInfo {
	return s.topologyUtils.CreateHost(instanceId, instanceName, isWriter, weight, lastUpdateTime, initialHost, instanceTemplate)
}
