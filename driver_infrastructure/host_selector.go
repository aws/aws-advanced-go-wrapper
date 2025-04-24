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
	"errors"
	"regexp"
	"strconv"
	"strings"
)

const (
	SELECTOR_RANDOM          = "random"
	SELECTOR_WEIGHTED_RANDOM = "weightedRandom"
)

var (
	HOST_WEIGHT_PAIR_PATTERN  = regexp.MustCompile("(?i)^((?P<host>[^:/?#]*):(?P<weight>[0-9]*))")
	HOST_PATTERN_GROUP        = "host"
	HOST_WEIGHT_PATTERN_GROUP = "weight"
)

type HostSelector interface {
	GetHost(hosts []*host_info_util.HostInfo, role host_info_util.HostRole, props map[string]string) (*host_info_util.HostInfo, error)
}

type WeightedHostSelector interface {
	SetHostWeights(hostWeightMap map[string]int)
	ClearHostWeights()
}

func GetHostWeightMapFromString(hostWeightMapString string) (map[string]int, error) {
	hostWeightMap := make(map[string]int)
	if strings.TrimSpace(hostWeightMapString) == "" {
		return hostWeightMap, nil
	}
	hostWeightPairSlice := strings.Split(hostWeightMapString, ",")
	for _, hostWeightPair := range hostWeightPairSlice {
		if HOST_WEIGHT_PAIR_PATTERN.MatchString(hostWeightPair) {
			hostName := strings.TrimSpace(HOST_WEIGHT_PAIR_PATTERN.FindStringSubmatch(hostWeightPair)[HOST_WEIGHT_PAIR_PATTERN.SubexpIndex(HOST_PATTERN_GROUP)])
			hostWeightString := strings.TrimSpace(HOST_WEIGHT_PAIR_PATTERN.FindStringSubmatch(hostWeightPair)[HOST_WEIGHT_PAIR_PATTERN.SubexpIndex(HOST_WEIGHT_PATTERN_GROUP)])
			if hostName == "" || hostWeightString == "" {
				return nil, errors.New(error_util.GetMessage("HostSelector.InvalidHostWeightPairs"))
			}
			hostWeight, err := strconv.Atoi(hostWeightString)
			if err != nil || hostWeight < 1 {
				return nil, errors.New(error_util.GetMessage("InvalidHostWeightPairs"))
			}
			hostWeightMap[hostName] = hostWeight
		} else {
			return nil, errors.New(error_util.GetMessage("HostSelector.InvalidHostWeightPairs"))
		}
	}
	return hostWeightMap, nil
}
