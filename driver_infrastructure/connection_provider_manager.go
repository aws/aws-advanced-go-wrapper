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

type ConnectionProviderManager struct {
	defaultProvider   *ConnectionProvider
	effectiveProvider *ConnectionProvider
}

func (connProviderManager *ConnectionProviderManager) GetConnectionProvider(
	hostInfo HostInfo,
	properties map[string]any) *ConnectionProvider {
	if connProviderManager.effectiveProvider != nil && (*connProviderManager.effectiveProvider).AcceptsUrl(hostInfo, properties) {
		return connProviderManager.effectiveProvider
	}

	return connProviderManager.defaultProvider
}

func (connProviderManager *ConnectionProviderManager) GetDefaultProvider() *ConnectionProvider {
	return connProviderManager.defaultProvider
}

func (connProviderManager *ConnectionProviderManager) AcceptsStrategy(role HostRole, strategy string) bool {
	if connProviderManager.effectiveProvider != nil && (*connProviderManager.effectiveProvider).AcceptsStrategy(role, strategy) {
		return true
	}

	return (*connProviderManager.defaultProvider).AcceptsStrategy(role, strategy)
}

func (connProviderManager *ConnectionProviderManager) GetHostInfoByStrategy(
	hosts []HostInfo,
	role HostRole,
	strategy string,
	properties map[string]any) (HostInfo, error) {
	if (*connProviderManager.effectiveProvider).AcceptsStrategy(role, strategy) {
		host, err := (*connProviderManager.effectiveProvider).GetHostInfoByStrategy(hosts, role, strategy, properties)
		if err == nil {
			return host, err
		}
	}

	return (*connProviderManager.defaultProvider).GetHostInfoByStrategy(hosts, role, strategy, properties)
}
