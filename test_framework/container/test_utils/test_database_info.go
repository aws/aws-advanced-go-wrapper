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
	"slices"
)

type TestDatabaseInfo struct {
	username                    string
	password                    string
	defaultDbName               string
	clusterEndpoint             string
	clusterEndpointPort         int
	clusterReadOnlyEndpoint     string
	clusterReadOnlyEndpointPort int
	instanceEndpointSuffix      string
	instanceEndpointPort        int
	instances                   []TestInstanceInfo
}

func (t TestDatabaseInfo) InstanceEndpointPort() int {
	return t.instanceEndpointPort
}

func (t TestDatabaseInfo) InstanceEndpointSuffix() string {
	return t.instanceEndpointSuffix
}

func (t *TestDatabaseInfo) moveInstanceFirst(instanceId string) {
	index := slices.IndexFunc(t.instances, func(info TestInstanceInfo) bool { return info.instanceId == instanceId })
	if index == 0 || index == -1 {
		// Given instance is either at the front or not in the slice at all.
		return
	}
	result := append(append(append(make([]TestInstanceInfo, 0, len(t.instances)), t.instances[index]), t.instances[:index]...), t.instances[index+1:]...)
	t.instances = result
}

func NewTestDatabaseInfo(databaseInfoMap map[string]any) (databaseInfo TestDatabaseInfo, err error) {
	instanceMaps, ok := databaseInfoMap["instances"].([]any)
	instances := make([]TestInstanceInfo, len(instanceMaps))
	for i, instanceMapVal := range instanceMaps {
		instanceMap, map_ok := instanceMapVal.(map[string]any)
		if map_ok {
			instances[i], err = NewTestInstanceInfo(instanceMap)
			if err != nil {
				return
			}
		}
	}
	username, ok1 := databaseInfoMap["username"].(string)
	password, ok2 := databaseInfoMap["password"].(string)
	defaultDbName, ok3 := databaseInfoMap["defaultDbName"].(string)
	clusterEndpoint, ok4 := databaseInfoMap["clusterEndpoint"].(string)
	clusterReadOnlyEndpoint, ok5 := databaseInfoMap["clusterReadOnlyEndpoint"].(string)
	instanceEndpointSuffix, ok6 := databaseInfoMap["instanceEndpointSuffix"].(string)
	clusterEndpointPort, ok7 := databaseInfoMap["clusterEndpointPort"].(float64)
	clusterReadOnlyEndpointPort, ok8 := databaseInfoMap["clusterReadOnlyEndpointPort"].(float64)
	instanceEndpointPort, ok9 := databaseInfoMap["instanceEndpointPort"].(float64)

	if ok && ok1 && ok2 && ok3 && ok4 && ok5 && ok6 && ok7 && ok8 && ok9 {
		return TestDatabaseInfo{
			username:                    username,
			password:                    password,
			defaultDbName:               defaultDbName,
			clusterEndpoint:             clusterEndpoint,
			clusterEndpointPort:         int(clusterEndpointPort),
			clusterReadOnlyEndpoint:     clusterReadOnlyEndpoint,
			clusterReadOnlyEndpointPort: int(clusterReadOnlyEndpointPort),
			instanceEndpointSuffix:      instanceEndpointSuffix,
			instanceEndpointPort:        int(instanceEndpointPort),
			instances:                   instances,
		}, nil
	}
	err = errors.New("Unable to cast a database info value")
	return
}
