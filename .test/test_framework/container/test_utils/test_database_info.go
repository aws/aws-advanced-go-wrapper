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
	Username                    string
	Password                    string
	DefaultDbName               string
	ClusterEndpoint             string
	ClusterEndpointPort         int
	ClusterReadOnlyEndpoint     string
	clusterReadOnlyEndpointPort int
	InstanceEndpointSuffix      string
	InstanceEndpointPort        int
	BlueGreenDeploymentId       string
	Instances                   []TestInstanceInfo
}

func (t TestDatabaseInfo) WriterInstanceEndpoint() string {
	if len(t.Instances) > 0 && t.Instances[0].host != "" {
		return t.Instances[0].host
	}
	return ""
}

func (t TestDatabaseInfo) ReaderInstance() (info TestInstanceInfo) {
	if len(t.Instances) > 1 && t.Instances[1].host != "" && t.Instances[1].instanceId != "" {
		info = t.Instances[1]
	}
	return
}

// Creates a reordered slice of the instances in t where the instance matching instanceId is first.
// Updates either proxyTestDatabaseInfo or testDatabaseInfo of the testEnvironment variable to contain the reordered instances.
func (t TestDatabaseInfo) moveInstanceFirst(instanceId string, isProxyTestDatabaseInfo bool) {
	index := slices.IndexFunc(t.Instances, func(info TestInstanceInfo) bool { return info.instanceId == instanceId })
	if index == 0 || index == -1 {
		// Given instance is either at the front or not in the slice at all.
		return
	}
	reorderedInstances := append(append(append(make([]TestInstanceInfo, 0, len(t.Instances)), t.Instances[index]),
		t.Instances[:index]...), t.Instances[index+1:]...)

	// Update testEnvironment to have correct order.
	testEnvironmentMutex.Lock()
	defer testEnvironmentMutex.Unlock()
	if testEnvironment == nil {
		return
	}
	if isProxyTestDatabaseInfo {
		testEnvironment.info.ProxyDatabaseInfo.Instances = reorderedInstances
	} else {
		testEnvironment.info.DatabaseInfo.Instances = reorderedInstances
	}
}

func NewTestDatabaseInfo(databaseInfoMap map[string]any) (databaseInfo TestDatabaseInfo, err error) {
	instanceMaps, ok := databaseInfoMap["instances"].([]any)
	instances := make([]TestInstanceInfo, len(instanceMaps))
	for i, instanceMapVal := range instanceMaps {
		instanceMap, mapOk := instanceMapVal.(map[string]any)
		if mapOk {
			instances[i], err = NewTestInstanceInfo(instanceMap)
			if err != nil {
				return
			}
		}
	}
	username, ok1 := databaseInfoMap["username"].(string)
	password, ok2 := databaseInfoMap["password"].(string)
	defaultDbName, ok3 := databaseInfoMap["defaultDbName"].(string)
	clusterEndpoint, _ := databaseInfoMap["clusterEndpoint"].(string)
	clusterReadOnlyEndpoint, _ := databaseInfoMap["clusterReadOnlyEndpoint"].(string)
	instanceEndpointSuffix, ok4 := databaseInfoMap["instanceEndpointSuffix"].(string)
	clusterEndpointPort, _ := databaseInfoMap["clusterEndpointPort"].(float64)
	clusterReadOnlyEndpointPort, ok5 := databaseInfoMap["clusterReadOnlyEndpointPort"].(float64)
	instanceEndpointPort, ok6 := databaseInfoMap["instanceEndpointPort"].(float64)
	blueGreenDeploymentId, _ := databaseInfoMap["blueGreenDeploymentId"].(string)

	if ok && ok1 && ok2 && ok3 && ok4 && ok5 && ok6 {
		return TestDatabaseInfo{
			Username:                    username,
			Password:                    password,
			DefaultDbName:               defaultDbName,
			ClusterEndpoint:             clusterEndpoint,
			ClusterEndpointPort:         int(clusterEndpointPort),
			ClusterReadOnlyEndpoint:     clusterReadOnlyEndpoint,
			clusterReadOnlyEndpointPort: int(clusterReadOnlyEndpointPort),
			InstanceEndpointSuffix:      instanceEndpointSuffix,
			InstanceEndpointPort:        int(instanceEndpointPort),
			BlueGreenDeploymentId:       blueGreenDeploymentId,
			Instances:                   instances,
		}, nil
	}
	err = errors.New("unable to cast a database info value")
	return
}
