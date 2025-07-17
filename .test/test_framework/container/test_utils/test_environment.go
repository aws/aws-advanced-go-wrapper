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
		return nil, errors.New("unable to get environment variable TEST_ENV_INFO_JSON")
	}
	var testInfo map[string]any
	err := json.Unmarshal([]byte(infoJson), &testInfo)
	if err != nil {
		return nil, errors.New("unable to parse json")
	}
	env, err := NewTestEnvironment(testInfo)
	if err != nil {
		return nil, err
	}
	if slices.Contains(env.info.Request.Features, NETWORK_OUTAGES_ENABLED) {
		err = initProxies(env)
		if err != nil {
			return nil, err
		}
	}
	return env, nil
}

func (t TestEnvironment) GetProxy(instanceId string) ProxyInfo {
	return t.proxies[instanceId]
}

func (t TestEnvironment) ProxyInfos() map[string]ProxyInfo {
	return t.proxies
}

func (t TestEnvironment) Info() TestEnvironmentInfo {
	return t.info
}

func VerifyClusterStatus() error {
	env, err := GetCurrentTestEnvironment()
	if err != nil {
		return err
	}
	info := env.info
	if info.Request.Deployment == AURORA || info.Request.Deployment == RDS_MULTI_AZ_CLUSTER || info.Request.Deployment == AURORA_LIMITLESS {
		remainingTries := 3
		success := false
		for !success && remainingTries > 0 {
			remainingTries--
			auroraUtility := NewAuroraTestUtility(info.Region)
			err := auroraUtility.waitUntilClusterHasDesiredStatus(info.auroraClusterName, "available")
			if err != nil {
				rebootAllClusterInstances()
				break
			}

			// Limitless doesn't have instances
			if info.Request.Deployment != AURORA_LIMITLESS {
				writerId, err := auroraUtility.GetClusterWriterInstanceId(info.auroraClusterName)
				if err != nil {
					rebootAllClusterInstances()
					break
				}
				info.DatabaseInfo.moveInstanceFirst(writerId, false)
				info.ProxyDatabaseInfo.moveInstanceFirst(writerId, true)
			}

			success = true
		}
		if !success {
			return errors.New("cluster is not healthy")
		}
	}

	return nil
}

func rebootAllClusterInstances() {
	env, err := GetCurrentTestEnvironment()
	if err != nil {
		slog.Warn(fmt.Sprintf("unable to reboot with no test environment, error: %s", err.Error()))
	}
	info := env.info
	auroraUtility := NewAuroraTestUtility(info.Region)
	if info.auroraClusterName == "" {
		return
	}
	waitForClusterAndLogError(auroraUtility, info.auroraClusterName)

	for _, instance := range info.DatabaseInfo.Instances {
		auroraUtility.rebootInstance(instance.instanceId)
	}
	waitForClusterAndLogError(auroraUtility, info.auroraClusterName)

	for _, instance := range info.DatabaseInfo.Instances {
		waitForClusterAndLogError(auroraUtility, instance.instanceId)
	}
}

func waitForClusterAndLogError(auroraUtility *AuroraTestUtility, instanceId string) {
	err := auroraUtility.waitUntilInstanceHasDesiredStatus(instanceId, "available")
	if err != nil {
		slog.Warn(fmt.Sprintf("Issue waiting for instance %s to be available. Error: %s.", instanceId, err.Error()))
	}
}
