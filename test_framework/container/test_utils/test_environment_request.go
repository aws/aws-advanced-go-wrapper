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
)

type TestEnvironmentRequest struct {
	InstanceCount int
	instances     DatabaseInstances
	Engine        DatabaseEngine
	Deployment    DatabaseEngineDeployment
	features      []TestEnvironmentFeatures
}

func NewTestEnvironmentRequest(requestVal any) (request TestEnvironmentRequest, err error) {
	requestMap, ok := requestVal.(map[string]any)
	if !ok {
		err = fmt.Errorf("Unable to cast request value to usable map: %s", requestMap)
		return
	}
	instanceCountAsFloat, ok := requestMap["numOfInstances"].(float64)
	instanceCount := 1
	if ok && instanceCountAsFloat > 1 {
		instanceCount = int(instanceCountAsFloat)
	}
	var instances DatabaseInstances
	instancesVal := requestMap["instances"]
	if instancesVal == "SINGLE_INSTANCE" {
		instances = SINGLE_INSTANCE
	} else if instancesVal == "MULTI_INSTANCE" {
		instances = MULTI_INSTANCE
	} else {
		err = fmt.Errorf("Invalid instances: %s", instancesVal)
		return
	}
	var engine DatabaseEngine
	engineVal := requestMap["engine"]
	if engineVal == "MYSQL" {
		engine = MYSQL
	} else if engineVal == "PG" {
		engine = PG
	} else {
		err = fmt.Errorf("Invalid engine: %s", engineVal)
		return
	}
	var deployment DatabaseEngineDeployment
	deploymentVal := requestMap["deployment"]
	if deploymentVal == "DOCKER" {
		deployment = DOCKER
	} else if deploymentVal == "RDS" {
		deployment = RDS
	} else if deploymentVal == "AURORA" {
		deployment = AURORA
	} else {
		err = fmt.Errorf("Invalid deployment: %s", deploymentVal)
		return
	}

	features := []TestEnvironmentFeatures{}
	featureStrings, ok := requestMap["features"].([]any)
	if ok && len(featureStrings) > 0 {
		features = matchFeatures(featureStrings)
	}

	return TestEnvironmentRequest{
		instances:     instances,
		InstanceCount: instanceCount,
		Engine:        engine,
		Deployment:    deployment,
		features:      features,
	}, nil
}
