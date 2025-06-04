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
	"fmt"

	_ "github.com/Shopify/toxiproxy/client"
)

type TestProxyDatabaseInfo struct {
	controlPort int
	TestDatabaseInfo
}

func NewTestProxyDatabaseInfo(databaseInfoMap map[string]any) (TestProxyDatabaseInfo, error) {
	super, err := NewTestDatabaseInfo(databaseInfoMap)
	controlPort := -1
	if err == nil {
		controlPortFloat, ok := databaseInfoMap["controlPort"].(float64)
		if ok {
			controlPort = int(controlPortFloat)
		} else {
			err = fmt.Errorf("No control port value for new proxy database info %s.", super.ClusterEndpoint)
		}
	}
	return TestProxyDatabaseInfo{controlPort, super}, err
}

func (t TestProxyDatabaseInfo) ControlPort() int {
	return t.controlPort
}
