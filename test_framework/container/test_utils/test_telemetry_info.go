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

package test_utils

import (
	"errors"
	"strconv"
)

type TestTelemetryInfo struct {
	Endpoint     string
	EndpointPort string
}

func NewTestTelemetryInfo(info map[string]any) (*TestTelemetryInfo, error) {
	endpoint, ok1 := info["endpoint"].(string)
	endpointPort, ok2 := info["endpointPort"].(float64)
	if !ok1 || !ok2 {
		return nil, errors.New("endpoint or endpointPort not found or could not convert to a string")
	}
	return &TestTelemetryInfo{
		endpoint,
		strconv.Itoa(int(endpointPort)),
	}, nil
}
