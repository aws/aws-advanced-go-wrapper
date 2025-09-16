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
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

const ROUND_ROBIN_DEFAULT_WEIGHT = 1
const ROUND_ROBIN_DEFAULT_CACHE_EXPIRE_NANO = time.Duration(10) * time.Minute

var roundRobinCache *utils.CacheMap[*RoundRobinClusterInfo]

type RoundRobinHostSelector struct {
	mu sync.RWMutex
}

type RoundRobinClusterInfo struct {
	lastHost                               *host_info_util.HostInfo
	clusterWeightsMap                      map[string]int
	defaultWeight                          int
	weightCounter                          int
	lastClusterHostWeightPairPropertyValue string
	lastClusterDefaultWeightPropertyValue  int
}

func NewRoundRobinHostSelector() *RoundRobinHostSelector {
	if roundRobinCache == nil {
		roundRobinCache = utils.NewCache[*RoundRobinClusterInfo]()
	}
	return &RoundRobinHostSelector{}
}

func (r *RoundRobinHostSelector) GetHost(hosts []*host_info_util.HostInfo, role host_info_util.HostRole, props map[string]string) (*host_info_util.HostInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	eligibleHosts := utils.FilterSlice(hosts, func(hostInfo *host_info_util.HostInfo) bool {
		return role == hostInfo.Role && hostInfo.Availability == host_info_util.AVAILABLE
	})

	slices.SortFunc(eligibleHosts, func(i, j *host_info_util.HostInfo) int {
		return strings.Compare(i.GetHost(), j.GetHost())
	})

	if len(eligibleHosts) == 0 {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("HostSelector.noHostsMatchingRole", role))
	}

	err := r.createCacheEntryForHosts(eligibleHosts, props)
	if err != nil {
		return nil, err
	}
	currentClusterInfoKey := eligibleHosts[0].GetHost()
	clusterInfo, found := roundRobinCache.Get(currentClusterInfoKey)

	if !found {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("HostSelector.roundRobinMissingClusterInfo", currentClusterInfoKey))
	}

	lastHost := clusterInfo.lastHost
	lastHostIndex := -1

	// Check if lastHost is in list of eligible hosts. Update lastHostIndex
	if lastHost != nil {
		lastHostIndex = slices.IndexFunc(eligibleHosts, func(hostInfo *host_info_util.HostInfo) bool {
			return hostInfo.GetHost() == lastHost.GetHost()
		})
	}
	var targetHostIndex int

	// If the host is weighted and the lastHost is in the eligibleHosts list.
	if clusterInfo.weightCounter > 0 && lastHostIndex != -1 {
		targetHostIndex = lastHostIndex
	} else {
		if lastHostIndex != len(eligibleHosts)-1 {
			targetHostIndex = lastHostIndex + 1
		} else {
			targetHostIndex = 0
		}

		weight := clusterInfo.clusterWeightsMap[eligibleHosts[targetHostIndex].HostId]
		if weight == 0 {
			clusterInfo.weightCounter = clusterInfo.defaultWeight
		} else {
			clusterInfo.weightCounter = weight
		}
	}

	clusterInfo.weightCounter -= 1
	clusterInfo.lastHost = eligibleHosts[targetHostIndex]
	return eligibleHosts[targetHostIndex], nil
}

func (r *RoundRobinHostSelector) createCacheEntryForHosts(hosts []*host_info_util.HostInfo, props map[string]string) error {
	hostsWithCacheEntry := []*host_info_util.HostInfo{}

	for _, host := range hosts {
		_, found := roundRobinCache.Get(host.GetHost())
		if found {
			hostsWithCacheEntry = append(hostsWithCacheEntry, host)
		}
	}

	// If there is a host with an existing entry, update the cache entries for all hosts to point each to the same
	// RoundRobinClusterInfo object. If there are no cache entries, create a new RoundRobinClusterInfo
	if len(hostsWithCacheEntry) > 0 {
		roundRobinClusterInfo, found := roundRobinCache.Get(hostsWithCacheEntry[0].GetHost())

		if found &&
			r.hasPropertyChanged(roundRobinClusterInfo.lastClusterHostWeightPairPropertyValue, property_util.ROUND_ROBIN_HOST_WEIGHT_PAIRS, props) {
			roundRobinClusterInfo.lastHost = nil
			roundRobinClusterInfo.weightCounter = 0
			err := r.updateCachedHostWeightPairsPropsForRoundRobinClusterInfo(roundRobinClusterInfo, props)
			if err != nil {
				return err
			}
		}

		if found &&
			r.hasPropertyChanged(strconv.Itoa(roundRobinClusterInfo.lastClusterDefaultWeightPropertyValue), property_util.ROUND_ROBIN_DEFAULT_WEIGHT, props) {
			roundRobinClusterInfo.lastHost = nil
			roundRobinClusterInfo.weightCounter = 0
			err := r.updateCachedDefaultWeightPropsForRoundRobinClusterInfo(roundRobinClusterInfo, props)
			if err != nil {
				return err
			}
		}
		for _, host := range hosts {
			roundRobinCache.Put(host.GetHost(), roundRobinClusterInfo, ROUND_ROBIN_DEFAULT_CACHE_EXPIRE_NANO)
		}
	} else {
		roundRobinClusterInfo := RoundRobinClusterInfo{
			clusterWeightsMap:                      make(map[string]int),
			defaultWeight:                          ROUND_ROBIN_DEFAULT_WEIGHT,
			weightCounter:                          0,
			lastClusterHostWeightPairPropertyValue: property_util.ROUND_ROBIN_HOST_WEIGHT_PAIRS.Get(props),
			lastClusterDefaultWeightPropertyValue:  ROUND_ROBIN_DEFAULT_WEIGHT,
		}
		err := r.updateCachePropertiesForRoundRobinClusterInfo(&roundRobinClusterInfo, props)
		if err != nil {
			return err
		}
		for _, host := range hosts {
			roundRobinCache.Put(host.GetHost(), &roundRobinClusterInfo, ROUND_ROBIN_DEFAULT_CACHE_EXPIRE_NANO)
		}
	}
	return nil
}

func (r *RoundRobinHostSelector) hasPropertyChanged(
	lastPropertyValue string,
	wrapperProperty property_util.AwsWrapperProperty,
	props map[string]string) bool {
	if props == nil {
		return false
	}
	propValue := wrapperProperty.Get(props)
	return propValue != lastPropertyValue
}

func (r *RoundRobinHostSelector) updateCachePropertiesForRoundRobinClusterInfo(
	roundRobinClusterInfo *RoundRobinClusterInfo,
	props map[string]string) error {
	err := r.updateCachedDefaultWeightPropsForRoundRobinClusterInfo(roundRobinClusterInfo, props)
	if err != nil {
		return err
	}

	err = r.updateCachedHostWeightPairsPropsForRoundRobinClusterInfo(roundRobinClusterInfo, props)
	if err != nil {
		return err
	}

	return nil
}

func (r *RoundRobinHostSelector) updateCachedDefaultWeightPropsForRoundRobinClusterInfo(
	roundRobinClusterInfo *RoundRobinClusterInfo,
	props map[string]string) error {
	defaultWeight := ROUND_ROBIN_DEFAULT_WEIGHT

	if props != nil {
		weightProps := property_util.ROUND_ROBIN_DEFAULT_WEIGHT.Get(props)
		convertedWeight, err := strconv.Atoi(weightProps)
		if err != nil || convertedWeight <= 0 {
			return errors.New(error_util.GetMessage("HostSelector.roundRobinInvalidDefaultWeight"))
		}
		roundRobinClusterInfo.lastClusterDefaultWeightPropertyValue = convertedWeight
		defaultWeight = convertedWeight
	}

	roundRobinClusterInfo.defaultWeight = defaultWeight
	return nil
}

func (r *RoundRobinHostSelector) updateCachedHostWeightPairsPropsForRoundRobinClusterInfo(
	roundRobinClusterInfo *RoundRobinClusterInfo,
	props map[string]string) error {
	if props == nil {
		return nil
	}

	hostWeights := property_util.ROUND_ROBIN_HOST_WEIGHT_PAIRS.Get(props)

	if hostWeights == "" {
		clear(roundRobinClusterInfo.clusterWeightsMap)
		roundRobinClusterInfo.lastClusterHostWeightPairPropertyValue = property_util.ROUND_ROBIN_HOST_WEIGHT_PAIRS.Get(props)
		return nil
	}

	hostWeightPairSlice := strings.Split(hostWeights, ",")
	if len(hostWeightPairSlice) == 0 {
		return errors.New(error_util.GetMessage("HostSelector.invalidHostWeightPairs"))
	}

	hostWeightMap := make(map[string]int)
	for _, hostWeightPair := range hostWeightPairSlice {
		if HOST_WEIGHT_PAIR_PATTERN.MatchString(hostWeightPair) {
			hostName := strings.TrimSpace(HOST_WEIGHT_PAIR_PATTERN.FindStringSubmatch(hostWeightPair)[HOST_WEIGHT_PAIR_PATTERN.SubexpIndex(HOST_PATTERN_GROUP)])
			hostWeightString := strings.TrimSpace(HOST_WEIGHT_PAIR_PATTERN.FindStringSubmatch(hostWeightPair)[HOST_WEIGHT_PAIR_PATTERN.SubexpIndex(HOST_WEIGHT_PATTERN_GROUP)])
			if hostName == "" || hostWeightString == "" {
				return errors.New(error_util.GetMessage("HostSelector.invalidHostWeightPairs"))
			}
			hostWeight, err := strconv.Atoi(hostWeightString)
			if err != nil || hostWeight < ROUND_ROBIN_DEFAULT_WEIGHT {
				return errors.New(error_util.GetMessage("HostSelector.invalidHostWeightPairs"))
			}
			hostWeightMap[hostName] = hostWeight
			roundRobinClusterInfo.clusterWeightsMap[hostName] = hostWeight
		} else {
			return errors.New(error_util.GetMessage("HostSelector.invalidHostWeightPairs"))
		}
	}
	return nil
}

// For testing purposes only.
func (r *RoundRobinHostSelector) ClearCache() {
	r.mu.Lock()
	defer r.mu.Unlock()
	roundRobinCache.Clear()
}
