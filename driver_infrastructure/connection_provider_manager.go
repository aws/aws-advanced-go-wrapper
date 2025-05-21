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
	"awssql/host_info_util"
)

type ConnectionProviderManager struct {
	DefaultProvider   ConnectionProvider
	EffectiveProvider ConnectionProvider
}

func (connProviderManager *ConnectionProviderManager) GetConnectionProvider(
	hostInfo host_info_util.HostInfo,
	props map[string]string) ConnectionProvider {
	if connProviderManager.EffectiveProvider != nil && connProviderManager.EffectiveProvider.AcceptsUrl(hostInfo, props) {
		return connProviderManager.EffectiveProvider
	}

	return connProviderManager.DefaultProvider
}

func (connProviderManager *ConnectionProviderManager) GetDefaultProvider() ConnectionProvider {
	return connProviderManager.DefaultProvider
}

func (connProviderManager *ConnectionProviderManager) AcceptsStrategy(strategy string) bool {
	if connProviderManager.EffectiveProvider != nil && connProviderManager.EffectiveProvider.AcceptsStrategy(strategy) {
		return true
	}

	return connProviderManager.DefaultProvider.AcceptsStrategy(strategy)
}

func (connProviderManager *ConnectionProviderManager) GetHostInfoByStrategy(
	hosts []*host_info_util.HostInfo,
	role host_info_util.HostRole,
	strategy string,
	props map[string]string) (*host_info_util.HostInfo, error) {
	if connProviderManager.EffectiveProvider != nil && connProviderManager.EffectiveProvider.AcceptsStrategy(strategy) {
		host, err := connProviderManager.EffectiveProvider.GetHostInfoByStrategy(hosts, role, strategy, props)
		if err == nil {
			return host, err
		}
	}

	return connProviderManager.DefaultProvider.GetHostInfoByStrategy(hosts, role, strategy, props)
}

func (connProviderManager *ConnectionProviderManager) GetHostSelectorStrategy(strategy string) (HostSelector, error) {
	if connProviderManager.EffectiveProvider != nil && connProviderManager.EffectiveProvider.AcceptsStrategy(strategy) {
		hostSelector, err := connProviderManager.EffectiveProvider.GetHostSelectorStrategy(strategy)
		if err == nil {
			return hostSelector, err
		}
	}

	return connProviderManager.DefaultProvider.GetHostSelectorStrategy(strategy)
}
