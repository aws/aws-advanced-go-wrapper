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
	"log/slog"
)

type DatabaseInstances string

const (
	SINGLE_INSTANCE DatabaseInstances = "SINGLE_INSTANCE"
	MULTI_INSTANCE  DatabaseInstances = "MULTI_INSTANCE"
)

type DatabaseEngineDeployment string

const (
	DOCKER                DatabaseEngineDeployment = "DOCKER"
	RDS                   DatabaseEngineDeployment = "RDS"
	AURORA                DatabaseEngineDeployment = "AURORA"
	RDS_MULTI_AZ_CLUSTER  DatabaseEngineDeployment = "RDS_MULTI_AZ_CLUSTER"
	RDS_MULTI_AZ_INSTANCE DatabaseEngineDeployment = "RDS_MULTI_AZ_INSTANCE"
	AURORA_LIMITLESS      DatabaseEngineDeployment = "AURORA_LIMITLESS"
)

type DatabaseEngine string

const (
	MYSQL DatabaseEngine = "MYSQL"
	PG    DatabaseEngine = "PG"
)

type TestEnvironmentFeatures string

const (
	IAM                            TestEnvironmentFeatures = "IAM"
	SECRETS_MANAGER                TestEnvironmentFeatures = "SECRETS_MANAGER"
	FAILOVER_SUPPORTED             TestEnvironmentFeatures = "FAILOVER_SUPPORTED"
	ABORT_CONNECTION_SUPPORTED     TestEnvironmentFeatures = "ABORT_CONNECTION_SUPPORTED"
	NETWORK_OUTAGES_ENABLED        TestEnvironmentFeatures = "NETWORK_OUTAGES_ENABLED"
	AWS_CREDENTIALS_ENABLED        TestEnvironmentFeatures = "AWS_CREDENTIALS_ENABLED"
	PERFORMANCE                    TestEnvironmentFeatures = "PERFORMANCE"
	RACE                           TestEnvironmentFeatures = "RACE"
	RUN_AUTOSCALING_TESTS_ONLY     TestEnvironmentFeatures = "RUN_AUTOSCALING_TESTS_ONLY"
	SKIP_MYSQL_DRIVER_TESTS        TestEnvironmentFeatures = "SKIP_MYSQL_DRIVER_TESTS"
	SKIP_PG_DRIVER_TESTS           TestEnvironmentFeatures = "SKIP_PG_DRIVER_TESTS"
	RDS_MULTI_AZ_CLUSTER_SUPPORTED TestEnvironmentFeatures = "RDS_MULTI_AZ_CLUSTER_SUPPORTED"
	LIMITLESS_DEPLOYMENT           TestEnvironmentFeatures = "LIMITLESS_DEPLOYMENT"
	BLUE_GREEN_DEPLOYMENT          TestEnvironmentFeatures = "BLUE_GREEN_DEPLOYMENT"
	TELEMETRY_METRICS_ENABLED      TestEnvironmentFeatures = "TELEMETRY_METRICS_ENABLED"
	TELEMETRY_TRACES_ENABLED       TestEnvironmentFeatures = "TELEMETRY_TRACES_ENABLED"
)

var stringToTestEnvironmentFeature = map[string]TestEnvironmentFeatures{
	"IAM":                            IAM,
	"SECRETS_MANAGER":                SECRETS_MANAGER,
	"FAILOVER_SUPPORTED":             FAILOVER_SUPPORTED,
	"ABORT_CONNECTION_SUPPORTED":     ABORT_CONNECTION_SUPPORTED,
	"NETWORK_OUTAGES_ENABLED":        NETWORK_OUTAGES_ENABLED,
	"AWS_CREDENTIALS_ENABLED":        AWS_CREDENTIALS_ENABLED,
	"PERFORMANCE":                    PERFORMANCE,
	"RUN_AUTOSCALING_TESTS_ONLY":     RUN_AUTOSCALING_TESTS_ONLY,
	"SKIP_MYSQL_DRIVER_TESTS":        SKIP_MYSQL_DRIVER_TESTS,
	"SKIP_PG_DRIVER_TESTS":           SKIP_PG_DRIVER_TESTS,
	"RDS_MULTI_AZ_CLUSTER_SUPPORTED": RDS_MULTI_AZ_CLUSTER_SUPPORTED,
	"LIMITLESS_DEPLOYMENT":           LIMITLESS_DEPLOYMENT,
	"BLUE_GREEN_DEPLOYMENT":          BLUE_GREEN_DEPLOYMENT,
	"TELEMETRY_METRICS_ENABLED":      TELEMETRY_METRICS_ENABLED,
	"TELEMETRY_TRACES_ENABLED":       TELEMETRY_TRACES_ENABLED,
}

func matchFeatures(featureStrings []any) []TestEnvironmentFeatures {
	features := make([]TestEnvironmentFeatures, len(featureStrings))
	for i, feature := range featureStrings {
		featureString, ok := feature.(string)
		if !ok {
			slog.Debug(fmt.Sprintf("issue casting %v", feature))
			continue
		}
		testEnvironmentFeature, ok := stringToTestEnvironmentFeature[featureString]
		if !ok {
			slog.Debug(fmt.Sprintf("invalid feature %s", featureString))
			continue
		}
		features[i] = testEnvironmentFeature
	}
	return features
}
