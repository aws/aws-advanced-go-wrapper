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

// Compile-time interface checks.
var (
	_ HostListProvider         = (*GlobalAuroraHostListProvider)(nil)
	_ BlockingHostListProvider = (*GlobalAuroraHostListProvider)(nil)
)

// GlobalAuroraHostListProvider provides host list management for Global Aurora
// Databases that span multiple AWS regions. It composes with RdsHostListProvider
// for shared infrastructure but creates its own monitor with a global-aware
// topology query strategy.
type GlobalAuroraHostListProvider struct {
	base                      *RdsHostListProvider
	globalTopologyUtils       RegionAwareTopologyUtils
	instanceTemplatesByRegion map[string]*host_info_util.HostInfo
	globalInitialized         bool
}

func NewGlobalAuroraHostListProvider(
	hostListProviderService HostListProviderService,
	topologyUtils RegionAwareTopologyUtils,
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

	g.base = NewRdsHostListProvider(
		hostListProviderService,
		topologyUtils,
		properties,
		servicesContainer,
		g.getOrCreateMonitor,
	)

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

func (g *GlobalAuroraHostListProvider) getOrCreateMonitor() (ClusterTopologyMonitor, error) {
	g.base.init()

	if err := g.initGlobal(); err != nil {
		return nil, err
	}

	highRefreshRateNano := time.Millisecond * time.Duration(
		property_util.GetRefreshRateValue(g.base.properties, property_util.CLUSTER_TOPOLOGY_HIGH_REFRESH_RATE_MS))

	strategy := &globalAuroraTopologyQueryStrategy{
		topologyUtils:             g.globalTopologyUtils,
		initialHostInfo:           g.base.initialHostInfo,
		clusterInstanceTemplate:   g.base.clusterInstanceTemplate,
		instanceTemplatesByRegion: g.instanceTemplatesByRegion,
	}

	initializer := func(container ServicesContainer) (Monitor, error) {
		monitor := NewClusterTopologyMonitorImpl(
			container,
			g.globalTopologyUtils,
			g.base.clusterId,
			highRefreshRateNano,
			g.base.refreshRateNanos,
			TOPOLOGY_CACHE_EXPIRATION_NANO,
			g.base.properties,
			g.base.initialHostInfo,
			g.base.clusterInstanceTemplate,
			g.base.servicesContainer.GetPluginService(),
			strategy)
		return monitor, nil
	}

	monitor, err := g.base.servicesContainer.GetMonitorService().RunIfAbsent(
		ClusterTopologyMonitorType,
		g.base.clusterId,
		g.base.servicesContainer,
		initializer)
	if err != nil {
		return nil, err
	}
	return monitor.(ClusterTopologyMonitor), nil
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

// =============================================================================
// BlockingHostListProvider interface
// =============================================================================

func (g *GlobalAuroraHostListProvider) ForceRefreshHostListWithTimeout(shouldVerifyWriter bool, timeoutMs int) ([]*host_info_util.HostInfo, error) {
	return g.base.ForceRefreshHostListWithTimeout(shouldVerifyWriter, timeoutMs)
}
