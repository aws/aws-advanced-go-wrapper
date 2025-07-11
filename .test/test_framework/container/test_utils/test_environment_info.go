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

type TestEnvironmentInfo struct {
	Request                 TestEnvironmentRequest
	Region                  string
	auroraClusterName       string
	databaseEngineVersion   string
	databaseEngine          string
	DatabaseInfo            TestDatabaseInfo
	ProxyDatabaseInfo       TestProxyDatabaseInfo
	IamUsername             string
	OtelTracesTelemetryInfo *TestTelemetryInfo
	XrayTracesTelemetryInfo *TestTelemetryInfo
	MetricsTelemetryInfo    *TestTelemetryInfo
}

func NewTestEnvironmentInfo(testInfo map[string]any) (info TestEnvironmentInfo, err error) {
	region, ok1 := testInfo["region"].(string)
	auroraClusterName, ok2 := testInfo["auroraClusterName"].(string)
	databaseEngine, ok3 := testInfo["databaseEngine"].(string)
	databaseEngineVersion, ok4 := testInfo["databaseEngineVersion"].(string)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		err = errors.New("unable to cast an environment info value")
		return
	}
	request, err := NewTestEnvironmentRequest(testInfo["request"])
	if err != nil {
		return
	}
	databaseInfoMap, ok := testInfo["databaseInfo"].(map[string]any)
	if !ok {
		err = errors.New("unable to cast databaseInfo value to usable map")
		return
	}
	databaseInfo, err := NewTestDatabaseInfo(databaseInfoMap)
	if err != nil {
		return
	}

	var proxyDatabaseInfo TestProxyDatabaseInfo
	if slices.Contains(request.Features, NETWORK_OUTAGES_ENABLED) {
		proxyDatabaseInfoMap, ok := testInfo["proxyDatabaseInfo"].(map[string]any)
		if !ok {
			err = errors.New("unable to cast proxyDatabaseInfo value to usable map")
			return
		}
		proxyDatabaseInfo, err = NewTestProxyDatabaseInfo(proxyDatabaseInfoMap)
		if err != nil {
			return
		}
	}

	iamUser, ok := testInfo["iamUsername"].(string)

	if !ok {
		err = errors.New("unable to get IAM username")
		return
	}

	otelTracesTelemetryInfoMap, ok := testInfo["otelTracesTelemetryInfo"].(map[string]any)
	if !ok {
		return TestEnvironmentInfo{}, errors.New("unable to cast otelTracesTelemetryInfo value to usable map")
	}
	otelTracesTelemetryInfo, err := NewTestTelemetryInfo(otelTracesTelemetryInfoMap)
	if err != nil {
		return
	}

	xrayTracesTelemetryInfoMap, ok := testInfo["xrayTracesTelemetryInfo"].(map[string]any)
	if !ok {
		return TestEnvironmentInfo{}, errors.New("unable to cast xrayTracesTelemetryInfo value to usable map")
	}
	xrayTracesTelemetryInfo, err := NewTestTelemetryInfo(xrayTracesTelemetryInfoMap)
	if err != nil {
		return
	}

	metricsTelemetryInfoMap, ok := testInfo["metricsTelemetryInfo"].(map[string]any)
	if !ok {
		return TestEnvironmentInfo{}, errors.New("unable to cast metricsTelemetryInfo value to usable map")
	}
	metricsTelemetryInfo, err := NewTestTelemetryInfo(metricsTelemetryInfoMap)
	if err != nil {
		return
	}

	return TestEnvironmentInfo{
		Request:                 request,
		Region:                  region,
		auroraClusterName:       auroraClusterName,
		DatabaseInfo:            databaseInfo,
		ProxyDatabaseInfo:       proxyDatabaseInfo,
		databaseEngine:          databaseEngine,
		databaseEngineVersion:   databaseEngineVersion,
		IamUsername:             iamUser,
		OtelTracesTelemetryInfo: otelTracesTelemetryInfo,
		XrayTracesTelemetryInfo: xrayTracesTelemetryInfo,
		MetricsTelemetryInfo:    metricsTelemetryInfo,
	}, nil
}

func (t TestEnvironmentInfo) AuroraClusterName() string {
	return t.auroraClusterName
}
