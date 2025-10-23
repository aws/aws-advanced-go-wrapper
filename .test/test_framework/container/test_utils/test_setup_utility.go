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
	"strconv"
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
)

func GetPropsForProxy(env *TestEnvironment, host, plugins string, timeout int) map[string]string {
	var hostValue string
	if host == "" {
		hostValue = env.Info().ProxyDatabaseInfo.ClusterEndpoint
	} else {
		hostValue = host
	}
	timeoutStr := strconv.Itoa(timeout - 1)
	monitoringParam := property_util.MONITORING_PROPERTY_PREFIX
	var driverProtocol, timeoutParam string

	switch env.Info().Request.Engine {
	case PG:
		timeoutParam = "connect_timeout"
		driverProtocol = property_util.PGX_DRIVER_PROTOCOL
	case MYSQL:
		timeoutParam = "readTimeout"
		timeoutStr += "s"
		driverProtocol = property_util.MYSQL_DRIVER_PROTOCOL
	}

	return map[string]string{
		"host":                             hostValue,
		"port":                             strconv.Itoa(env.Info().ProxyDatabaseInfo.InstanceEndpointPort),
		"clusterInstanceHostPattern":       "?." + env.Info().ProxyDatabaseInfo.InstanceEndpointSuffix,
		"plugins":                          plugins,
		"failureDetectionIntervalMs":       strconv.Itoa(timeout * 1000),
		"failureDetectionCount":            "3",
		"failureDetectionTimeMs":           "1000",
		"failoverTimeoutMs":                "15000",
		monitoringParam + timeoutParam:     timeoutStr,
		property_util.DRIVER_PROTOCOL.Name: driverProtocol,
	}
}

func GetPropsForProxyWithConnectTimeout(env *TestEnvironment, host, plugins string, timeout int) map[string]string {
	props := GetPropsForProxy(env, host, plugins, timeout)
	return AddDriverConnectTimeoutToProps(env, props, timeout)
}

func AddDriverConnectTimeoutToProps(env *TestEnvironment, props map[string]string, timeout int) map[string]string {
	timeoutStr := strconv.Itoa(timeout - 1)
	var timeoutParam string
	switch env.Info().Request.Engine {
	case PG:
		timeoutParam = "connect_timeout"
	case MYSQL:
		timeoutParam = "readTimeout"
		timeoutStr += "s"
	}
	props[timeoutParam] = timeoutStr
	return props
}

func SkipIfInsufficientInstances(t *testing.T, env *TestEnvironment, minInstances int) {
	if env.Info().Request.InstanceCount < minInstances {
		t.Skipf("Skipping integration test %s, instanceCount = %v.", t.Name(), env.Info().Request.InstanceCount)
	}
}
