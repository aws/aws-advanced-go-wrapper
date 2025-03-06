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

package driver

import "testing"

func TestGetHostGivenUnavailableHost(t *testing.T) {
	for i := 0; i < 50; i++ {
		props := map[string]any{}

		unavailableHost := HostInfo{Host: "someUnavailableHost", Role: READER, Availability: UNAVAILABLE}
		availableHost := HostInfo{Host: "someAvailableHost", Role: READER, Availability: AVAILABLE}
		hosts := []HostInfo{unavailableHost, availableHost}

		hostSelector := RandomHostSelector{}

		actualHost, err := hostSelector.GetHost(hosts, READER, props)

		if err != nil {
			t.Fatalf("Unexpected error getting host: %v on iteration %v", err, i)
		}
		if actualHost.Equals(availableHost) == false {
			t.Fatalf("Expected host %v but got %v on iteration %v", availableHost, actualHost, i)
		}
	}
}

func TestGetHostGivenMultipleUnavailableHosts(t *testing.T) {
	for i := 0; i < 50; i++ {
		props := map[string]any{}

		hosts := []HostInfo{
			{Host: "someUnavailableHost", Role: READER, Availability: UNAVAILABLE},
			{Host: "someUnavailableHost", Role: READER, Availability: UNAVAILABLE},
			{Host: "someAvailableHost", Role: READER, Availability: AVAILABLE},
			{Host: "someAvailableHost", Role: READER, Availability: AVAILABLE},
		}

		hostSelector := RandomHostSelector{}

		actualHost, err := hostSelector.GetHost(hosts, READER, props)

		if err != nil {
			t.Fatalf("Unexpected error getting host: %v on iteration %v", err, i)
		}
		if actualHost.Availability != AVAILABLE {
			t.Fatalf("Expected available host on iteration %v", i)
		}
	}
}
