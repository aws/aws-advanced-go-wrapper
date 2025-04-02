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

package test

import (
	"awssql/driver_infrastructure"
	"awssql/host_info_util"
	"testing"
)

func TestGetHostGivenUnavailableHost(t *testing.T) {
	for i := 0; i < 50; i++ {
		props := map[string]string{}

		unavailableHost := &host_info_util.HostInfo{Host: "someUnavailableHost", Role: host_info_util.READER, Availability: host_info_util.UNAVAILABLE}
		availableHost := &host_info_util.HostInfo{Host: "someAvailableHost", Role: host_info_util.READER, Availability: host_info_util.AVAILABLE}
		hosts := []*host_info_util.HostInfo{unavailableHost, availableHost}

		hostSelector := driver_infrastructure.RandomHostSelector{}

		actualHost, err := hostSelector.GetHost(hosts, host_info_util.READER, props)

		if err != nil {
			t.Fatalf("Unexpected error getting host: %v on iteration %v", err, i)
		}
		if actualHost.Equals(*availableHost) == false {
			t.Fatalf("Expected host %v but got %v on iteration %v", availableHost, actualHost, i)
		}
	}
}

func TestGetHostGivenMultipleUnavailableHosts(t *testing.T) {
	for i := 0; i < 50; i++ {
		props := map[string]string{}

		hosts := []*host_info_util.HostInfo{
			{Host: "someUnavailableHost", Role: host_info_util.READER, Availability: host_info_util.UNAVAILABLE},
			{Host: "someUnavailableHost", Role: host_info_util.READER, Availability: host_info_util.UNAVAILABLE},
			{Host: "someAvailableHost", Role: host_info_util.READER, Availability: host_info_util.AVAILABLE},
			{Host: "someAvailableHost", Role: host_info_util.READER, Availability: host_info_util.AVAILABLE},
		}

		hostSelector := driver_infrastructure.RandomHostSelector{}

		actualHost, err := hostSelector.GetHost(hosts, host_info_util.READER, props)

		if err != nil {
			t.Fatalf("Unexpected error getting host: %v on iteration %v", err, i)
		}
		if actualHost.Availability != host_info_util.AVAILABLE {
			t.Fatalf("Expected available host on iteration %v", i)
		}
	}
}
