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

package host_info_util

import (
	"fmt"
	"slices"
	"strings"
)

func AreHostListsEqual(s1 []*HostInfo, s2 []*HostInfo) bool {
	if len(s1) != len(s2) {
		return false
	}

	for i := 0; i < len(s1); i++ {
		if !s1[i].Equals(s2[i]) {
			return false
		}
	}

	return true
}

func GetWriter(hosts []*HostInfo) *HostInfo {
	for _, host := range hosts {
		if host != nil && host.Role == WRITER {
			return host
		}
	}
	return nil
}

func GetReaders(hosts []*HostInfo) []*HostInfo {
	readerHosts := make([]*HostInfo, 0, len(hosts))
	for _, host := range hosts {
		if host != nil && host.Role == READER {
			readerHosts = append(readerHosts, host)
		}
	}
	slices.SortFunc(readerHosts, func(i, j *HostInfo) int {
		return strings.Compare(i.GetHost(), j.GetHost())
	})
	return readerHosts
}

func GetHostAndPort(host string, port int) string {
	if port > 0 && host != "" {
		return fmt.Sprintf("%s:%d", host, port)
	}
	return host
}

func HaveNoHostsInCommon(hosts1 []*HostInfo, hosts2 []*HostInfo) bool {
	var mapSlice, checkSlice []*HostInfo
	if len(hosts1) <= len(hosts2) {
		mapSlice, checkSlice = hosts1, hosts2
	} else {
		mapSlice, checkSlice = hosts2, hosts1
	}

	checkMap := make(map[string]int, len(mapSlice))
	for _, host := range mapSlice {
		checkMap[host.Host] = 0
	}

	for _, host := range checkSlice {
		if _, exists := checkMap[host.Host]; exists {
			return false
		}
	}
	return true
}
