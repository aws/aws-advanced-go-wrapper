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
	"math/rand"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

type RandomHostSelector struct{}

func (r *RandomHostSelector) GetHost(hosts []*host_info_util.HostInfo, role host_info_util.HostRole, props map[string]string) (*host_info_util.HostInfo, error) {
	eligibleHosts := utils.FilterSlice(hosts, func(hostInfo *host_info_util.HostInfo) bool {
		return role == hostInfo.Role && hostInfo.Availability == host_info_util.AVAILABLE
	})

	if len(eligibleHosts) == 0 {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("HostSelector.noHostsMatchingRole", role))
	}

	randomIndex := rand.Intn(len(eligibleHosts))
	return eligibleHosts[randomIndex], nil
}
