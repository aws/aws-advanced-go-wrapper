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
	"reflect"
)

type DriverConnectionProvider struct {
	acceptedStrategies map[string]HostSelector
	targetDriver       driver.Driver
}

func NewDriverConnectionProvider(targetDriver driver.Driver) *DriverConnectionProvider {
	acceptedStrategies := make(map[string]HostSelector)
	acceptedStrategies[SELECTOR_RANDOM] = &RandomHostSelector{}
	return &DriverConnectionProvider{acceptedStrategies, targetDriver}
}

func (d DriverConnectionProvider) AcceptsUrl(hostInfo HostInfo, props map[string]string) bool {
	return true
}

func (d DriverConnectionProvider) AcceptsStrategy(role HostRole, strategy string) bool {
	_, ok := d.acceptedStrategies[strategy]
	return ok
}

func (d DriverConnectionProvider) GetHostInfoByStrategy(
	hosts []HostInfo,
	role HostRole,
	strategy string,
	props map[string]string) (HostInfo, error) {
	acceptedStrategy, ok := d.acceptedStrategies[strategy]
	if !ok {
		return HostInfo{}, NewUnsupportedStrategyError(
			GetMessage("ConnectionProvider.unsupportedHostSelectorStrategy", strategy, reflect.TypeOf(d).String()))
	}

	return acceptedStrategy.GetHost(hosts, role, props)
}

func (d DriverConnectionProvider) Connect(hostInfo HostInfo, props map[string]string, pluginService *PluginService) (driver.Conn, error) {
	targetDriverDialect := (*pluginService).GetTargetDriverDialect()
	dsn := targetDriverDialect.GetDsnFromProperties(props)
	conn, err := d.targetDriver.Open(dsn)
	//nolint:all
	if err != nil {
		// TODO: green node replacement
	}
	return conn, err
}
