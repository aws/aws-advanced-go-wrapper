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
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
)

type DriverConnectionProvider struct {
	acceptedStrategies map[string]HostSelector
	targetDriver       driver.Driver
}

func NewDriverConnectionProvider(targetDriver driver.Driver) *DriverConnectionProvider {
	acceptedStrategies := make(map[string]HostSelector)
	acceptedStrategies[SELECTOR_HIGHEST_WEIGHT] = &HighestWeightHostSelector{}
	acceptedStrategies[SELECTOR_RANDOM] = &RandomHostSelector{}
	acceptedStrategies[SELECTOR_WEIGHTED_RANDOM] = &WeightedRandomHostSelector{}
	return &DriverConnectionProvider{acceptedStrategies, targetDriver}
}

func (d DriverConnectionProvider) AcceptsUrl(hostInfo host_info_util.HostInfo, props map[string]string) bool {
	return true
}

func (d DriverConnectionProvider) AcceptsStrategy(strategy string) bool {
	_, ok := d.acceptedStrategies[strategy]
	return ok
}

func (d DriverConnectionProvider) GetHostInfoByStrategy(
	hosts []*host_info_util.HostInfo,
	role host_info_util.HostRole,
	strategy string,
	props map[string]string) (*host_info_util.HostInfo, error) {
	acceptedStrategy, ok := d.acceptedStrategies[strategy]
	if !ok {
		return nil, error_util.NewUnsupportedStrategyError(
			error_util.GetMessage("ConnectionProvider.unsupportedHostSelectorStrategy", strategy, d))
	}

	return acceptedStrategy.GetHost(hosts, role, props)
}

func (d DriverConnectionProvider) GetHostSelectorStrategy(strategy string) (HostSelector, error) {
	acceptedStrategy, ok := d.acceptedStrategies[strategy]
	if !ok {
		return nil, error_util.NewUnsupportedStrategyError(
			error_util.GetMessage("ConnectionProvider.unsupportedHostSelectorStrategy", strategy, d))
	}
	return acceptedStrategy, nil
}

func (d DriverConnectionProvider) Connect(hostInfo *host_info_util.HostInfo, props map[string]string, pluginService PluginService) (driver.Conn, error) {
	targetDriverDialect := pluginService.GetTargetDriverDialect()
	dsn := targetDriverDialect.PrepareDsn(props, hostInfo)
	conn, err := d.targetDriver.Open(dsn)
	return conn, err
}
