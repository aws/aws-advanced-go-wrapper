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
	"awssql/utils"
	"errors"
)

type HighestWeightHostSelector struct{}

func (h *HighestWeightHostSelector) GetHost(
	hosts []*host_info_util.HostInfo,
	role host_info_util.HostRole,
	props map[string]string) (*host_info_util.HostInfo, error) {
	eligibleHosts := utils.FilterSlice(hosts, func(hostInfo *host_info_util.HostInfo) bool {
		return role == hostInfo.Role && hostInfo.Availability == host_info_util.AVAILABLE
	})

	if len(eligibleHosts) == 0 {
		return nil, errors.New(error_util.GetMessage("HighestWeightHostSelector.noHostsMatchingRole", role))
	}

	currHighestWeightHost := eligibleHosts[0]
	for _, host := range hosts {
		if host.Weight > currHighestWeightHost.Weight {
			currHighestWeightHost = host
		}
	}
	return currHighestWeightHost, nil
}
