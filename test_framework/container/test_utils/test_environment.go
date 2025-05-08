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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
)

var testEnvironment *TestEnvironment = nil
var testEnvironmentMutex = sync.RWMutex{}

type TestEnvironment struct {
	info    TestEnvironmentInfo
	proxies map[string]ProxyInfo
}

func NewTestEnvironment(testInfo map[string]any) (*TestEnvironment, error) {
	info, err := NewTestEnvironmentInfo(testInfo)
	return &TestEnvironment{info: info, proxies: map[string]ProxyInfo{}}, err
}

func getTestEnvironment() *TestEnvironment {
	testEnvironmentMutex.RLock()
	defer testEnvironmentMutex.RUnlock()
	return testEnvironment
}

func setTestEnvironment(newTestEnvironment *TestEnvironment) *TestEnvironment {
	testEnvironmentMutex.Lock()
	defer testEnvironmentMutex.Unlock()
	testEnvironment = newTestEnvironment
	return testEnvironment
}

func GetCurrentTestEnvironment() (*TestEnvironment, error) {
	currentTestEnvironment := getTestEnvironment()
	if currentTestEnvironment == nil {
		newTestEnvironment, err := CreateTestEnvironment()
		if err != nil {
			return nil, err
		}
		currentTestEnvironment = setTestEnvironment(newTestEnvironment)
	}
	return currentTestEnvironment, nil
}

func CreateTestEnvironment() (*TestEnvironment, error) {
	infoJson, ok := os.LookupEnv("TEST_ENV_INFO_JSON")
	if !ok {
		return nil, errors.New("Unable to get environment variable TEST_ENV_INFO_JSON")
	}
	var testInfo map[string]any
	err := json.Unmarshal([]byte(infoJson), &testInfo)
	if err != nil {
		return nil, errors.New("Unable to parse json")
	}
	env, err := NewTestEnvironment(testInfo)
	if err != nil {
		return nil, err
	}
	if slices.Contains(env.info.request.features, NETWORK_OUTAGES_ENABLED) {
		err = initProxies(env)
		if err != nil {
			return nil, err
		}
	}
	return env, nil
}

func (t TestEnvironment) Region() string {
	return t.info.region
}

func (t TestEnvironment) ProxyInfos() map[string]ProxyInfo {
	return t.proxies
}

func (t TestEnvironment) Engine() DatabaseEngine {
	return t.info.request.engine
}

func (t TestEnvironment) Deployment() DatabaseEngineDeployment {
	return t.info.request.deployment
}

func (t TestEnvironment) User() string {
	return t.info.databaseInfo.username
}

func (t TestEnvironment) Password() string {
	return t.info.databaseInfo.password
}

func (t TestEnvironment) DefaultDbName() string {
	return t.info.databaseInfo.defaultDbName
}

func (t TestEnvironment) ClusterEndpoint() string {
	return t.info.databaseInfo.clusterEndpoint
}

func (t TestEnvironment) InstanceEndpointPort() int {
	return t.info.databaseInfo.instanceEndpointPort
}

func (t TestEnvironment) Instances() []TestInstanceInfo {
	return t.info.databaseInfo.instances
}

func (t TestEnvironment) ClusterReadOnlyEndpoint() string {
	return t.info.databaseInfo.clusterReadOnlyEndpoint
}

func (t TestEnvironment) ProxyInstances() []TestInstanceInfo {
	return t.info.proxyDatabaseInfo.instances
}

func (t TestEnvironment) ProxyDatabaseInfo() TestProxyDatabaseInfo {
	return t.info.proxyDatabaseInfo
}

func VerifyClusterStatus() error {
	env, err := GetCurrentTestEnvironment()
	if err != nil {
		return err
	}
	info := env.info
	if info.request.deployment == AURORA { // TODO || info.request.deployment == RDS_MULTI_AZ_CLUSTER {
		remainingTries := 3
		success := false
		for !success && remainingTries > 0 {
			remainingTries--
			auroraUtility := NewAuroraTestUtility(info.region)
			err := auroraUtility.waitUntilClusterHasDesiredStatus(info.auroraClusterName, "available")
			if err != nil {
				rebootAllClusterInstances()
				break
			}
			writerId, err := auroraUtility.getClusterWriterInstanceId(info.auroraClusterName)
			if err != nil {
				rebootAllClusterInstances()
				break
			}
			info.databaseInfo.moveInstanceFirst(writerId, false)
			info.proxyDatabaseInfo.moveInstanceFirst(writerId, true)

			success = true
		}
		if !success {
			return errors.New("Cluster is not healthy")
		}
	}

	return nil
}

func rebootAllClusterInstances() {
	env, err := GetCurrentTestEnvironment()
	if err != nil {
		slog.Warn(fmt.Sprintf("Unable to reboot with no test environment. Error: %s.", err.Error()))
	}
	info := env.info
	auroraUtility := NewAuroraTestUtility(info.region)
	if info.auroraClusterName == "" {
		return
	}
	waitForClusterAndLogError(auroraUtility, info.auroraClusterName)

	for _, instance := range info.databaseInfo.instances {
		auroraUtility.rebootInstance(instance.instanceId)
	}
	waitForClusterAndLogError(auroraUtility, info.auroraClusterName)

	for _, instance := range info.databaseInfo.instances {
		waitForClusterAndLogError(auroraUtility, instance.instanceId)
	}
}

func waitForClusterAndLogError(auroraUtility *AuroraTestUtility, instanceId string) {
	err := auroraUtility.waitUntilInstanceHasDesiredStatus(instanceId, "available")
	if err != nil {
		slog.Warn(fmt.Sprintf("Issue waiting for instance %s to be available. Error: %s.", instanceId, err.Error()))
	}
}
