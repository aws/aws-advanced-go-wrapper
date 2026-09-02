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

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	mock_telemetry "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/util/telemetry"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils"
	custom_endpoint "github.com/aws/aws-advanced-go-wrapper/custom-endpoint"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCustomEndpointPluginSubscribesToDispatchableConnectMethod is the regression guard for the
// defect this change fixes.
//
// The plugin subscribed to the literal "Connect" while PluginManagerImpl dispatches the connect
// pipeline under CONNECT_METHOD ("Conn.Connect"), and makePluginChain selects plugins by exact string
// match. The plugin was therefore dropped from the connect pipeline with no error and no log line: no
// monitor was created, DescribeDBClusterEndpoints was never called, and no host filtering was ever
// applied. Execute was subscribed correctly but opens by returning early while customEndpointHostInfo
// is nil, and only Connect assigns it, so the whole plugin was a silent no-op.
//
// Asserting against the constant rather than a string literal is the point: the regression came from a
// refactor that dropped the plugin_helpers import and inlined the constant's *name* instead of its
// value. A literal here would have been equally wrong and would not have caught it.
func TestCustomEndpointPluginSubscribesToDispatchableConnectMethod(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockMonitorService := mock_driver_infrastructure.NewMockMonitorService(ctrl)
	mockTelemetryFactory := mock_telemetry.NewMockTelemetryFactory(ctrl)
	mockTelemetryCounter := mock_telemetry.NewMockTelemetryCounter(ctrl)

	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()
	mockContainer.EXPECT().GetMonitorService().Return(mockMonitorService).AnyTimes()
	mockMonitorService.EXPECT().RegisterMonitorType(
		gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	mockPluginService.EXPECT().GetTelemetryFactory().Return(mockTelemetryFactory).AnyTimes()
	mockTelemetryFactory.EXPECT().CreateCounter(custom_endpoint.TELEMETRY_WAIT_FOR_INFO_COUNTER).
		Return(mockTelemetryCounter, nil)

	rdsClientFunc := func(*host_info_util.HostInfo, *utils.RWMap[string, string]) (*rds.Client, error) {
		return nil, nil
	}

	plugin, err := custom_endpoint.NewCustomEndpointPlugin(
		mockContainer, rdsClientFunc, utils.NewRWMap[string, string]())
	require.NoError(t, err)

	subscribed := plugin.GetSubscribedMethods()

	assert.Contains(t, subscribed, plugin_helpers.CONNECT_METHOD,
		"the plugin is not subscribed to the method name the connect pipeline dispatches, so it is "+
			"silently excluded from that pipeline and applies no host filtering at all")
	assert.NotContains(t, subscribed, "Connect",
		`"Connect" is not a dispatched method name; the connect pipeline uses "Conn.Connect"`)
}
