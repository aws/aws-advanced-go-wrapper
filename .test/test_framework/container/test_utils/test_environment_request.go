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
	Features      []TestEnvironmentFeatures
}

func NewTestEnvironmentRequest(requestVal any) (request TestEnvironmentRequest, err error) {
	requestMap, ok := requestVal.(map[string]any)
	if !ok {
		err = fmt.Errorf("unable to cast request value to usable map: %s", requestMap)
		return
	}
	instanceCountAsFloat, ok := requestMap["numOfInstances"].(float64)
	instanceCount := 1
	if ok && instanceCountAsFloat > 1 {
		instanceCount = int(instanceCountAsFloat)
	}
	var instances DatabaseInstances
	instancesVal := requestMap["instances"]
	switch instancesVal {
	case string(SINGLE_INSTANCE):
		instances = SINGLE_INSTANCE
	case string(MULTI_INSTANCE):
		instances = MULTI_INSTANCE
	default:
		err = fmt.Errorf("invalid instances: %s", instancesVal)
		return
	}
	var engine DatabaseEngine
	engineVal := requestMap["engine"]
	switch engineVal {
	case string(MYSQL):
		engine = MYSQL
	case string(PG):
		engine = PG
	default:
		err = fmt.Errorf("invalid engine: %s", engineVal)
		return
	}
	var deployment DatabaseEngineDeployment
	deploymentVal := requestMap["deployment"]
	switch deploymentVal {
	case string(DOCKER):
		deployment = DOCKER
	case string(RDS):
		deployment = RDS
	case string(AURORA):
		deployment = AURORA
	case string(AURORA_LIMITLESS):
		deployment = AURORA_LIMITLESS
	case string(RDS_MULTI_AZ_CLUSTER):
		deployment = RDS_MULTI_AZ_CLUSTER
	case string(RDS_MULTI_AZ_INSTANCE):
		deployment = RDS_MULTI_AZ_INSTANCE
	default:
		err = fmt.Errorf("invalid deployment: %s", deploymentVal)
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
		Features:      features,
	}, nil
}
