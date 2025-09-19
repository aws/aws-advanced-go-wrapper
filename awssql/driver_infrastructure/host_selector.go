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
	"regexp"
	"strconv"
	"strings"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
)

const (
	SELECTOR_HIGHEST_WEIGHT  = "highestWeight"
	SELECTOR_RANDOM          = "random"
	SELECTOR_WEIGHTED_RANDOM = "weightedRandom"
	SELECTOR_ROUND_ROBIN     = "roundRobin"
)

var (
	// Example host weight pair patterns: "host0:3,host1:02,host2:1", "host.com:50000".
	HOST_WEIGHT_PAIR_PATTERN  = regexp.MustCompile("^(?P<host>[^:]+):(?P<weight>[0-9]+)$")
	HOST_PATTERN_GROUP        = "host"
	HOST_WEIGHT_PATTERN_GROUP = "weight"
)

type HostSelector interface {
	GetHost(hosts []*host_info_util.HostInfo, role host_info_util.HostRole, props map[string]string) (*host_info_util.HostInfo, error)
}

type WeightedHostSelector interface {
	SetHostWeights(hostWeightMap map[string]int)
	ClearHostWeights()
	HostSelector
}

func GetHostWeightMapFromString(hostWeightMapString string) (map[string]int, error) {
	hostWeightMap := make(map[string]int)
	if strings.TrimSpace(hostWeightMapString) == "" {
		return hostWeightMap, nil
	}
	hostWeightPairSlice := strings.Split(hostWeightMapString, ",")
	for _, hostWeightPair := range hostWeightPairSlice {
		hostWeightPair = strings.TrimSpace(hostWeightPair)
		if HOST_WEIGHT_PAIR_PATTERN.MatchString(hostWeightPair) {
			matches := HOST_WEIGHT_PAIR_PATTERN.FindStringSubmatch(hostWeightPair)
			hostName := matches[1]
			hostWeightString := matches[2]
			if hostName == "" || hostWeightString == "" {
				return nil, errors.New(error_util.GetMessage("HostSelector.invalidHostWeightPairs"))
			}
			hostWeight, err := strconv.Atoi(hostWeightString)
			if err != nil || hostWeight <= 0 {
				return nil, errors.New(error_util.GetMessage("HostSelector.invalidHostWeightPairs"))
			}
			hostWeightMap[hostName] = hostWeight
		} else {
			return nil, errors.New(error_util.GetMessage("HostSelector.invalidHostWeightPairs"))
		}
	}
	return hostWeightMap, nil
}
