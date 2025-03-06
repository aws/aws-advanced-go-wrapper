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

package driver

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

func (d DriverConnectionProvider) AcceptsUrl(hostInfo HostInfo, properties map[string]any) bool {
	return true
}

func (d DriverConnectionProvider) AcceptsStrategy(role HostRole, strategy string) bool {
	_, ok := d.acceptedStrategies[strategy]
	return ok
}

func (d DriverConnectionProvider) GetHostInfoByStrategy(hosts []HostInfo, role HostRole, strategy string, properties map[string]any) (HostInfo, error) {
	acceptedStrategy, ok := d.acceptedStrategies[strategy]
	if !ok {
		return HostInfo{}, NewUnsupportedStrategyError(strategy, reflect.TypeOf(d).String())
	}

	return acceptedStrategy.GetHost(hosts, role, properties)
}

func (d DriverConnectionProvider) Connect(hostInfo HostInfo, properties map[string]any) (*driver.Conn, error) {
	dsn := DsnFromProperties(properties)
	conn, err := d.targetDriver.Open(dsn)
	//nolint:all
	if err != nil {
		// TODO: green node replacement
	}
	return &conn, err
}
