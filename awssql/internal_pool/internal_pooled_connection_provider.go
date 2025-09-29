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

package internal_pool

import (
	"database/sql/driver"
	"time"

	awsDriver "github.com/aws/aws-advanced-go-wrapper/awssql/driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

type internalPoolKeyFunc func(*host_info_util.HostInfo, map[string]string) string

type InternalPooledConnectionProvider struct {
	acceptedStrategies     map[string]driver_infrastructure.HostSelector
	databasePools          *utils.SlidingExpirationCache[*InternalConnPool]
	internalPoolOptions    *InternalPoolConfig
	poolKeyFunc            internalPoolKeyFunc
	poolExpirationDuration time.Duration
}

var defaultPoolTimeout = time.Duration(30) * time.Minute

func NewInternalPooledConnectionProvider(internalPoolOptions *InternalPoolConfig,
	poolExpirationDuration time.Duration) *InternalPooledConnectionProvider {
	return NewInternalPooledConnectionProviderWithPoolKeyFunc(internalPoolOptions, poolExpirationDuration, nil)
}

func NewInternalPooledConnectionProviderWithPoolKeyFunc(internalPoolOptions *InternalPoolConfig,
	poolExpirationDuration time.Duration,
	poolKeyFunc internalPoolKeyFunc) *InternalPooledConnectionProvider {
	acceptedStrategies := make(map[string]driver_infrastructure.HostSelector)
	acceptedStrategies[driver_infrastructure.SELECTOR_HIGHEST_WEIGHT] =
		&driver_infrastructure.HighestWeightHostSelector{}
	acceptedStrategies[driver_infrastructure.SELECTOR_RANDOM] =
		&driver_infrastructure.RandomHostSelector{}
	acceptedStrategies[driver_infrastructure.SELECTOR_WEIGHTED_RANDOM] =
		&driver_infrastructure.WeightedRandomHostSelector{}
	acceptedStrategies[driver_infrastructure.SELECTOR_ROUND_ROBIN] =
		driver_infrastructure.GetRoundRobinHostSelector()

	var disposalFunc utils.DisposalFunc[*InternalConnPool] = func(pool *InternalConnPool) bool {
		_ = pool.Close()
		return true
	}
	if poolExpirationDuration == 0 {
		poolExpirationDuration = defaultPoolTimeout
	}
	return &InternalPooledConnectionProvider{
		acceptedStrategies,
		utils.NewSlidingExpirationCache("internalConnectionPool", disposalFunc),
		internalPoolOptions,
		poolKeyFunc,
		poolExpirationDuration}
}

func (p *InternalPooledConnectionProvider) AcceptsUrl(hostInfo host_info_util.HostInfo, _ map[string]string) bool {
	urlType := utils.IdentifyRdsUrlType(hostInfo.Host)
	return urlType.IsRds
}

func (p *InternalPooledConnectionProvider) AcceptsStrategy(strategy string) bool {
	_, exists := p.acceptedStrategies[strategy]
	return exists
}

func (p *InternalPooledConnectionProvider) GetHostInfoByStrategy(hosts []*host_info_util.HostInfo,
	role host_info_util.HostRole, strategy string, props map[string]string) (*host_info_util.HostInfo, error) {
	acceptedStrategy, err := p.GetHostSelectorStrategy(strategy)
	if err != nil {
		return nil, err
	}
	return acceptedStrategy.GetHost(hosts, role, props)
}

func (p *InternalPooledConnectionProvider) GetHostSelectorStrategy(strategy string) (driver_infrastructure.HostSelector, error) {
	acceptedStrategy, exists := p.acceptedStrategies[strategy]

	if !exists {
		return nil, error_util.NewGenericAwsWrapperError(
			error_util.GetMessage("ConnectionProvider.unsupportedHostSelectorStrategy", strategy, p))
	}
	return acceptedStrategy, nil
}

func (p *InternalPooledConnectionProvider) Connect(hostInfo *host_info_util.HostInfo,
	props map[string]string, pluginService driver_infrastructure.PluginService) (driver.Conn, error) {
	driverDialect := pluginService.GetTargetDriverDialect()
	dsn := driverDialect.PrepareDsn(props, hostInfo)
	driverName := driverDialect.GetDriverRegistrationName()
	underlyingDriver := awsDriver.GetUnderlyingDriver(driverName)

	computeFunc := func() *InternalConnPool {
		connFunc := func() (driver.Conn, error) {
			return underlyingDriver.Open(dsn)
		}
		pool := NewConnPool(connFunc, p.internalPoolOptions)
		return pool
	}
	key := p.getPoolKey(hostInfo, props)
	poolKey := NewPoolKey(hostInfo.GetUrl(), driverName, key)

	connPool := p.databasePools.ComputeIfAbsent(poolKey.String(),
		computeFunc, p.poolExpirationDuration)
	return connPool.Get()
}

func (p *InternalPooledConnectionProvider) getPoolKey(hostInfo *host_info_util.HostInfo, props map[string]string) string {
	if p.poolKeyFunc != nil {
		return p.poolKeyFunc(hostInfo, props)
	}

	user := property_util.GetVerifiedWrapperPropertyValueFromMap[string](props, property_util.USER)

	if user != "" {
		return user
	}
	return property_util.GetVerifiedWrapperPropertyValueFromMap[string](props, property_util.DB_USER)
}

func (p *InternalPooledConnectionProvider) ReleaseResources() {
	// Close all the pools
	p.databasePools.Clear()
}
