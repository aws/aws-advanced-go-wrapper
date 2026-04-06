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
	"time"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	mock_telemetry "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/util/telemetry"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/region_util"
	custom_endpoint "github.com/aws/aws-advanced-go-wrapper/custom-endpoint"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func createCustomEndpointMonitorMocks(ctrl *gomock.Controller) (
	*mock_driver_infrastructure.MockServicesContainer,
	*mock_driver_infrastructure.MockPluginService,
	*mock_telemetry.MockTelemetryCounter,
) {
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockContainer := mock_driver_infrastructure.NewMockServicesContainer(ctrl)
	mockContainer.EXPECT().GetPluginService().Return(mockPluginService).AnyTimes()
	mockTelemetryCounter := mock_telemetry.NewMockTelemetryCounter(ctrl)
	return mockContainer, mockPluginService, mockTelemetryCounter
}

func TestCustomEndpointMonitorImpl_NewMonitor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockContainer, _, mockCounter := createCustomEndpointMonitorMocks(ctrl)

	hostInfo, _ := host_info_util.NewHostInfoBuilder().
		SetHost("my-endpoint.cluster-custom-xyz.us-east-2.rds.amazonaws.com").
		SetPort(5432).Build()

	monitor := custom_endpoint.NewCustomEndpointMonitorImpl(
		mockContainer,
		hostInfo,
		"my-endpoint",
		region_util.Region("us-east-2"),
		5*time.Second,
		mockCounter,
		nil,
	)

	assert.NotNil(t, monitor)
	assert.True(t, monitor.CanDispose())
}

func TestCustomEndpointMonitorImpl_GetState_BeforeStart(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockContainer, _, mockCounter := createCustomEndpointMonitorMocks(ctrl)

	hostInfo, _ := host_info_util.NewHostInfoBuilder().
		SetHost("my-endpoint.cluster-custom-xyz.us-east-2.rds.amazonaws.com").
		SetPort(5432).Build()

	monitor := custom_endpoint.NewCustomEndpointMonitorImpl(
		mockContainer,
		hostInfo,
		"my-endpoint",
		region_util.Region("us-east-2"),
		5*time.Second,
		mockCounter,
		nil,
	)

	// Before Start(), state should be Stopped (default)
	assert.Equal(t, driver_infrastructure.MonitorStateStopped, monitor.GetState())
}

func TestCustomEndpointMonitorImpl_HasCustomEndpointInfo_NoCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockContainer, _, mockCounter := createCustomEndpointMonitorMocks(ctrl)

	hostInfo, _ := host_info_util.NewHostInfoBuilder().
		SetHost("test-no-cache.cluster-custom-xyz.us-east-2.rds.amazonaws.com").
		SetPort(5432).Build()

	monitor := custom_endpoint.NewCustomEndpointMonitorImpl(
		mockContainer,
		hostInfo,
		"test-no-cache",
		region_util.Region("us-east-2"),
		5*time.Second,
		mockCounter,
		nil,
	)

	// No cache entry exists, should return false
	assert.False(t, monitor.HasCustomEndpointInfo())
}

func TestCustomEndpointMonitorImpl_RequestCustomEndpointInfoUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockContainer, _, mockCounter := createCustomEndpointMonitorMocks(ctrl)

	hostInfo, _ := host_info_util.NewHostInfoBuilder().
		SetHost("test-update.cluster-custom-xyz.us-east-2.rds.amazonaws.com").
		SetPort(5432).Build()

	monitor := custom_endpoint.NewCustomEndpointMonitorImpl(
		mockContainer,
		hostInfo,
		"test-update",
		region_util.Region("us-east-2"),
		5*time.Second,
		mockCounter,
		nil,
	)

	// Should not panic when called
	monitor.RequestCustomEndpointInfoUpdate()
}

func TestCustomEndpointMonitorImpl_Close(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockContainer, _, mockCounter := createCustomEndpointMonitorMocks(ctrl)

	hostInfo, _ := host_info_util.NewHostInfoBuilder().
		SetHost("test-close.cluster-custom-xyz.us-east-2.rds.amazonaws.com").
		SetPort(5432).Build()

	monitor := custom_endpoint.NewCustomEndpointMonitorImpl(
		mockContainer,
		hostInfo,
		"test-close",
		region_util.Region("us-east-2"),
		5*time.Second,
		mockCounter,
		nil,
	)

	// Close should not panic
	monitor.Close()
}

func TestCustomEndpointMonitorImpl_GetLastActivityTimestampNanos(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockContainer, _, mockCounter := createCustomEndpointMonitorMocks(ctrl)

	hostInfo, _ := host_info_util.NewHostInfoBuilder().
		SetHost("test-timestamp.cluster-custom-xyz.us-east-2.rds.amazonaws.com").
		SetPort(5432).Build()

	monitor := custom_endpoint.NewCustomEndpointMonitorImpl(
		mockContainer,
		hostInfo,
		"test-timestamp",
		region_util.Region("us-east-2"),
		5*time.Second,
		mockCounter,
		nil,
	)

	// Before Start(), timestamp should be 0
	assert.Equal(t, int64(0), monitor.GetLastActivityTimestampNanos())
}

func TestClearCache(t *testing.T) {
	// Should not panic
	custom_endpoint.ClearCache()
}
