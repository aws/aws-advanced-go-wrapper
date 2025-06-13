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
	"errors"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"math/rand/v2"
)

type WeightedRandomHostSelector struct {
	hostWeightMap    map[string]int
	randomNumberFunc func(int) int
}

func (r *WeightedRandomHostSelector) GetHost(hosts []*host_info_util.HostInfo, role host_info_util.HostRole, props map[string]string) (*host_info_util.HostInfo, error) {
	if len(r.hostWeightMap) == 0 {
		hostWeightMap, err := GetHostWeightMapFromString(property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.WEIGHTED_RANDOM_HOST_WEIGHT_PAIRS))
		if err != nil {
			return nil, err
		}
		r.hostWeightMap = hostWeightMap
	}

	eligibleHosts := utils.FilterSlice(hosts, func(hostInfo *host_info_util.HostInfo) bool {
		return role == hostInfo.Role && hostInfo.Availability == host_info_util.AVAILABLE
	})
	if len(eligibleHosts) == 0 {
		return nil, errors.New(error_util.GetMessage("WeightedRandomHostSelector.noHostsMatchingRole", role))
	}

	hostWeightRangeMap := make(map[NumberRange]*host_info_util.HostInfo)
	count := 1
	for _, host := range eligibleHosts {
		hostWeight, hostWeightFound := r.hostWeightMap[host.Host]
		if hostWeightFound && hostWeight > 0 {
			rangeStart := count
			rangeEnd := count + hostWeight - 1
			hostWeightRangeMap[NumberRange{rangeStart, rangeEnd}] = host
			count = count + hostWeight
		} else {
			hostWeightRangeMap[NumberRange{count, count}] = host
			count++
		}
	}

	if r.randomNumberFunc == nil {
		r.randomNumberFunc = rand.IntN
	}
	randomNumber := r.randomNumberFunc(count-1) + 1
	for hostWeightRange, host := range hostWeightRangeMap {
		if hostWeightRange.IsInNumberRange(randomNumber) {
			return host, nil
		}
	}

	// this should not be reached
	return nil, errors.New(error_util.GetMessage("WeightedRandomHostSelector.unableToGetHost"))
}

func (r *WeightedRandomHostSelector) SetHostWeights(hostWeightMap map[string]int) {
	r.hostWeightMap = hostWeightMap
}

func (r *WeightedRandomHostSelector) ClearHostWeights() {
	r.hostWeightMap = nil
}

func (r *WeightedRandomHostSelector) SetRandomNumberFunc(f func(int) int) {
	r.randomNumberFunc = f
}

type NumberRange struct {
	Start int
	End   int
}

func (nr NumberRange) IsInNumberRange(n int) bool {
	return nr.Start <= n && n <= nr.End
}
