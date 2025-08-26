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

package bg

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

type BlueGreenInterimStatus struct {
	phase                            driver_infrastructure.BlueGreenPhase
	version                          string
	port                             int
	startTopology                    []*host_info_util.HostInfo
	endTopology                      []*host_info_util.HostInfo
	startIpAddressesByHostMap        map[string]string
	currentIpAddressesByHostMap      map[string]string
	hostNames                        map[string]bool
	allStartTopologyIpChanged        bool
	allStartTopologyEndpointsRemoved bool
	allTopologyChanged               bool
}

func (b *BlueGreenInterimStatus) IsZero() bool {
	return b == nil || (b.version == "" && b.port == 0 && b.phase.IsZero())
}

func (b *BlueGreenInterimStatus) String() string {
	var currentIpMapParts []string
	for key, value := range b.currentIpAddressesByHostMap {
		currentIpMapParts = append(currentIpMapParts, fmt.Sprintf("%s -> %s", key, value))
	}
	currentIpMap := strings.Join(currentIpMapParts, "\n   ")

	var startIpMapParts []string
	for key, value := range b.startIpAddressesByHostMap {
		startIpMapParts = append(startIpMapParts, fmt.Sprintf("%s -> %s", key, value))
	}
	startIpMap := strings.Join(startIpMapParts, "\n   ")

	allHostNamesStr := strings.Join(utils.AllKeys(b.hostNames), "\n   ")

	startTopologyStr := utils.LogTopology(b.startTopology, "")
	endTopologyStr := utils.LogTopology(b.endTopology, "")

	phaseStr := "<null>"
	if b.phase.GetName() != "" {
		phaseStr = b.phase.GetName()
	}

	emptyOrValue := func(s string) string {
		if strings.TrimSpace(s) == "" {
			return "-"
		}
		return s
	}

	return fmt.Sprintf("BlueGreenInterimStatus [\n"+
		" phase %s, \n"+
		" version '%s', \n"+
		" port %d, \n"+
		" hostNames:\n"+
		"   %s \n"+
		" Start %s \n"+
		" start IP map:\n"+
		"   %s \n"+
		" Current %s \n"+
		" current IP map:\n"+
		"   %s \n"+
		" allStartTopologyIpChanged: %t \n"+
		" allStartTopologyEndpointsRemoved: %t \n"+
		" allTopologyChanged: %t \n"+
		"]",
		phaseStr,
		b.version,
		b.port,
		emptyOrValue(allHostNamesStr),
		emptyOrValue(startTopologyStr),
		emptyOrValue(startIpMap),
		emptyOrValue(endTopologyStr),
		emptyOrValue(currentIpMap),
		b.allStartTopologyIpChanged,
		b.allStartTopologyEndpointsRemoved,
		b.allTopologyChanged)
}

func (b *BlueGreenInterimStatus) GetCustomHashCode() uint64 {
	result := getValueHash(1, b.phase.GetName())
	result = getValueHash(result, b.version)
	result = getValueHash(result, strconv.Itoa(b.port))
	result = getValueHash(result, strconv.FormatBool(b.allStartTopologyIpChanged))
	result = getValueHash(result, strconv.FormatBool(b.allStartTopologyEndpointsRemoved))
	result = getValueHash(result, strconv.FormatBool(b.allTopologyChanged))

	result = getValueHash(result, b.getHostNamesString())
	result = getValueHash(result, b.getTopologyString(b.startTopology))
	result = getValueHash(result, b.getTopologyString(b.endTopology))
	result = getValueHash(result, b.getIpMapString(b.startIpAddressesByHostMap))
	result = getValueHash(result, b.getIpMapString(b.currentIpAddressesByHostMap))

	return result
}

func (b *BlueGreenInterimStatus) getHostNamesString() string {
	if len(b.hostNames) == 0 {
		return ""
	}

	// Extract keys from map and sort them
	keys := make([]string, 0, len(b.hostNames))
	for key := range b.hostNames {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	return strings.Join(keys, ",")
}

func (b *BlueGreenInterimStatus) getTopologyString(topology []*host_info_util.HostInfo) string {
	if len(topology) == 0 {
		return ""
	}

	// Map each HostInfo to string and sort
	hostStrings := make([]string, len(topology))
	for i, hostInfo := range topology {
		hostStrings[i] = hostInfo.GetHostAndPort() + string(hostInfo.Role)
	}
	sort.Strings(hostStrings)

	return strings.Join(hostStrings, ",")
}

func (b *BlueGreenInterimStatus) getIpMapString(ipMap map[string]string) string {
	if len(ipMap) == 0 {
		return ""
	}

	// Convert map entries to strings and sort
	entries := make([]string, 0, len(ipMap))
	for key, value := range ipMap {
		entries = append(entries, key+value)
	}
	sort.Strings(entries)

	return strings.Join(entries, ",")
}

func getValueHash(currentHash uint64, val string) uint64 {
	// Use FNV-1a hash algorithm for string hashing
	h := fnv.New64a()
	_, err := h.Write([]byte(val))
	if err != nil {
		slog.Warn(error_util.GetMessage("BlueGreenDeployment.errorGeneratingHash", err))
		return 0
	}
	stringHash := h.Sum64()

	return currentHash*31 + stringHash
}
