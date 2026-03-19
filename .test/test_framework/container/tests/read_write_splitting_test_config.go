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

package test

import (
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/.test/test_framework/container/test_utils"
)

type ReadWriteSplittingPluginType int

const (
	ReadWriteSplittingPluginMode ReadWriteSplittingPluginType = iota
	GdbReadWriteSplittingPluginMode
)

type readWriteSplittingTestConfig struct {
	name           string
	pluginType     ReadWriteSplittingPluginType
	plugins        string
	setupFn        func(t *testing.T, env *test_utils.TestEnvironment) map[string]string
	setupProxiedFn func(t *testing.T, env *test_utils.TestEnvironment, host string, connectTimeout int) map[string]string
}

var readWriteSplittingConfigs = []readWriteSplittingTestConfig{
	{
		name:       "ReadWriteSplitting",
		pluginType: ReadWriteSplittingPluginMode,
		plugins:    "readWriteSplitting,efm,failover",
		setupFn: func(t *testing.T, env *test_utils.TestEnvironment) map[string]string {
			return map[string]string{"plugins": "readWriteSplitting,efm,failover"}
		},
		setupProxiedFn: func(t *testing.T, env *test_utils.TestEnvironment, host string, connectTimeout int) map[string]string {
			return test_utils.GetPropsForProxyWithConnectTimeout(env, host, "readWriteSplitting,efm,failover", connectTimeout)
		},
	},
	{
		name:       "GdbReadWriteSplitting",
		pluginType: GdbReadWriteSplittingPluginMode,
		plugins:    "gdbReadWriteSplitting,efm,gdbFailover",
		setupFn: func(t *testing.T, env *test_utils.TestEnvironment) map[string]string {
			return map[string]string{"plugins": "gdbReadWriteSplitting,efm,gdbFailover"}
		},
		setupProxiedFn: func(t *testing.T, env *test_utils.TestEnvironment, host string, connectTimeout int) map[string]string {
			props := test_utils.GetPropsForProxyWithConnectTimeout(env, host, "gdbReadWriteSplitting,efm,gdbFailover", connectTimeout)
			props["activeHomeFailoverMode"] = "strict-writer"
			props["inactiveHomeFailoverMode"] = "strict-writer"
			props["failoverTimeoutMs"] = "60000"
			return props
		},
	},
}
