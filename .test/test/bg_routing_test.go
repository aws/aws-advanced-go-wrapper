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
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	mock_telemetry "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/util/telemetry"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/bg"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBaseRouting(t *testing.T) {
	hostAndPort := "myapp-prod.cluster-abc123.us-east-1.rds.amazonaws.com:5432"
	role := driver_infrastructure.SOURCE

	routing := bg.NewBaseRouting(hostAndPort, role)

	assert.NotNil(t, routing, "Should create BaseRouting instance")
}

func TestBaseRoutingIsMatch(t *testing.T) {
	hostAndPort := "test-host:5432"
	role := driver_infrastructure.SOURCE
	routing := bg.NewBaseRouting(hostAndPort, role)

	matchingHost, _ := host_info_util.NewHostInfoBuilder().
		SetHost("test-host").
		SetPort(5432).
		Build()

	assert.True(t, routing.IsMatch(matchingHost, driver_infrastructure.SOURCE),
		"Should match exact host and role")

	assert.False(t, routing.IsMatch(matchingHost, driver_infrastructure.TARGET),
		"Should not match different role")

	differentHost, _ := host_info_util.NewHostInfoBuilder().
		SetHost("different-host").
		SetPort(5432).
		Build()

	assert.False(t, routing.IsMatch(differentHost, driver_infrastructure.SOURCE),
		"Should not match different host")

	emptyRouting := bg.NewBaseRouting("", driver_infrastructure.BlueGreenRole{})
	assert.True(t, emptyRouting.IsMatch(matchingHost, driver_infrastructure.SOURCE),
		"Empty routing should match any host and role")
}

func TestBaseRoutingString(t *testing.T) {
	hostAndPort := "test-host:5432"
	role := driver_infrastructure.SOURCE
	routing := bg.NewBaseRouting(hostAndPort, role)

	result := routing.String()
	assert.Contains(t, result, "Routing", "String should contain 'Routing'")
	assert.Contains(t, result, "test-host:5432", "String should contain host and port")
	assert.Contains(t, result, role.String(), "String should contain role")
}

func TestRejectConnectRouting(t *testing.T) {
	hostAndPort := "test-host:5432"
	role := driver_infrastructure.SOURCE

	routing := bg.NewRejectConnectRouting(hostAndPort, role)
	assert.NotNil(t, routing, "Should create RejectConnectRouting instance")

	hostInfo, _ := host_info_util.NewHostInfoBuilder().
		SetHost("test-host").
		SetPort(5432).
		Build()

	props := emptyProps

	conn, err := routing.Apply(nil, hostInfo, props, true, nil)
	assert.Nil(t, conn, "Should return nil connection")
	assert.NotNil(t, err, "Should return error")
	assert.Contains(t, err.Error(), "in progress", "Error should mention in progress")
}

func TestSubstituteConnectRouting(t *testing.T) {
	hostAndPort := "original-host:5432"
	role := driver_infrastructure.SOURCE

	substituteHost, _ := host_info_util.NewHostInfoBuilder().
		SetHost("substitute-host").
		SetPort(5432).
		Build()

	routing := bg.NewSubstituteConnectRouting(hostAndPort, role, substituteHost, []*host_info_util.HostInfo{}, nil)
	assert.NotNil(t, routing, "Should create SubstituteConnectRouting instance")

	result := routing.String()
	assert.Contains(t, result, "SubstituteConnectRouting", "String should contain routing type")
	assert.Contains(t, result, "substitute-host:5432", "String should contain substitute host")
}

func TestSubstituteConnectRoutingApply(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	hostAndPort := "original-host:5432"
	role := driver_infrastructure.SOURCE
	substituteHost, _ := host_info_util.NewHostInfoBuilder().
		SetHost("substitute-host").
		SetPort(5432).
		Build()
	mockDriverConn := MockDriverConn{}
	hostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("test-host").SetPort(3306).Build()

	t.Run("SubstituteHostIsIp", func(t *testing.T) {
		substituteHostIp, _ := host_info_util.NewHostInfoBuilder().
			SetHost("12.34.56.78").
			SetPort(5432).
			Build()
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockPluginService.EXPECT().Connect(substituteHostIp, nil, nil).Return(mockDriverConn, errors.New("ip connect"))

		routing := bg.NewSubstituteConnectRouting(hostAndPort, role, substituteHostIp, []*host_info_util.HostInfo{}, nil)
		assert.NotNil(t, routing, "Should create SubstituteConnectRouting instance")
		conn, err := routing.Apply(nil, hostInfo, nil, true, mockPluginService)
		require.NotNil(t, err, "Should return ip connect error")
		assert.Equal(t, "ip connect", err.Error(), "Should return ip connect error")
		assert.NotNil(t, conn, "Should create a connection")
	})

	t.Run("IamNotInUse", func(t *testing.T) {
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockPluginService.EXPECT().Connect(substituteHost, nil, nil).Return(mockDriverConn, errors.New("no iam connect"))
		mockPluginService.EXPECT().IsPluginInUse(driver_infrastructure.IAM_PLUGIN_CODE).Return(false)

		routing := bg.NewSubstituteConnectRouting(hostAndPort, role, substituteHost, []*host_info_util.HostInfo{}, nil)
		assert.NotNil(t, routing, "Should create SubstituteConnectRouting instance")
		conn, err := routing.Apply(nil, hostInfo, nil, true, mockPluginService)
		require.NotNil(t, err, "Should return direct connect error")
		assert.Equal(t, "no iam connect", err.Error(), "Should return direct connect error")
		assert.NotNil(t, conn, "Should create a connection")
	})

	t.Run("NilIamHosts", func(t *testing.T) {
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockPluginService.EXPECT().IsPluginInUse(driver_infrastructure.IAM_PLUGIN_CODE).Return(true)

		routing := bg.NewSubstituteConnectRouting(hostAndPort, role, substituteHost, []*host_info_util.HostInfo{}, nil)
		assert.NotNil(t, routing, "Should create SubstituteConnectRouting instance")
		conn, err := routing.Apply(nil, hostInfo, nil, true, mockPluginService)
		require.NotNil(t, err, "Should return requireIamHost error")
		assert.Equal(t, error_util.GetMessage("BlueGreenDeployment.requireIamHost"), err.Error())
		assert.Nil(t, conn, "Should fail to create a connection")
	})

	t.Run("UnsuccessfulConnect", func(t *testing.T) {
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockPluginService.EXPECT().Connect(gomock.Any(), gomock.Any(), nil).Return(nil, errors.New("unsuccessful connect"))
		mockPluginService.EXPECT().IsPluginInUse(driver_infrastructure.IAM_PLUGIN_CODE).Return(true)

		routing := bg.NewSubstituteConnectRouting(hostAndPort, role, nil, []*host_info_util.HostInfo{hostInfo}, nil)
		assert.NotNil(t, routing, "Should create SubstituteConnectRouting instance")
		conn, err := routing.Apply(nil, hostInfo, nil, true, mockPluginService)
		require.NotNil(t, err, "Should return error")
		assert.Equal(t, error_util.GetMessage("BlueGreenDeployment.inProgressCantOpenConnection", ""), err.Error())
		assert.Nil(t, conn, "Should fail to create a connection")
	})

	t.Run("SuccessfulConnect", func(t *testing.T) {
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockPluginService.EXPECT().Connect(gomock.Any(), gomock.Any(), nil).Return(mockDriverConn, nil)
		mockPluginService.EXPECT().IsPluginInUse(driver_infrastructure.IAM_PLUGIN_CODE).Return(true)
		var iamHost string
		iamSuccessfulConnectNotify := func(s string) {
			iamHost = s
		}

		routing := bg.NewSubstituteConnectRouting(hostAndPort, role, nil, []*host_info_util.HostInfo{nil, hostInfo}, iamSuccessfulConnectNotify)
		assert.NotNil(t, routing, "Should create SubstituteConnectRouting instance")
		conn, err := routing.Apply(nil, hostInfo, nil, true, mockPluginService)
		require.Nil(t, err)
		assert.NotNil(t, conn, "Should create a connection")
		assert.Equal(t, hostInfo.GetHost(), iamHost)
	})
}

func TestSuspendConnectRoutingApply(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTelemetry := mock_telemetry.NewMockTelemetryFactory(ctrl)
	mockTelemetryCtx := mock_telemetry.NewMockTelemetryContext(ctrl)

	ctxBefore := context.Background()
	mockTelemetry.EXPECT().
		OpenTelemetryContext(gomock.Any(), telemetry.NESTED, ctxBefore).
		Return(mockTelemetryCtx, ctxBefore).AnyTimes()
	mockTelemetryCtx.EXPECT().CloseContext().AnyTimes()

	hostAndPort := "original-host:5432"
	role := driver_infrastructure.SOURCE
	hostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("test-host").SetPort(3306).Build()
	bgId := "test-bg-deployment-123"
	bgInProgressStatus := driver_infrastructure.NewBgStatus(bgId, driver_infrastructure.IN_PROGRESS, nil, nil, nil, nil)
	bgPostStatus := driver_infrastructure.NewBgStatus(bgId, driver_infrastructure.POST, nil, nil, nil, nil)
	routing := bg.NewSuspendConnectRouting(hostAndPort, role, bgId)
	props := MakeMapFromKeysAndVals(
		property_util.BG_CONNECT_TIMEOUT_MS.Name, "45",
	)
	assert.NotNil(t, routing, "Should create SuspendConnectRouting instance")

	t.Run("NilBgStatus", func(t *testing.T) {
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockPluginService.EXPECT().GetBgStatus(bgId).Return(driver_infrastructure.BlueGreenStatus{}, false).AnyTimes()
		mockPluginService.EXPECT().GetTelemetryContext().Return(ctxBefore).Times(1)
		mockPluginService.EXPECT().GetTelemetryFactory().Return(mockTelemetry).Times(1)
		mockPluginService.EXPECT().SetTelemetryContext(ctxBefore).Times(2)

		start := time.Now()
		conn, err := routing.Apply(nil, hostInfo, props, true, mockPluginService)
		require.NotNil(t, err, "Should return error")
		assert.True(t, strings.Contains(err.Error(), "Blue/Green Deployment switchover is completed. Continue with connect call."))
		assert.Nil(t, conn, "Should not create a connection")

		elapsed := time.Since(start)
		assert.True(t, elapsed <= 45*time.Millisecond, "Should not sleep for the requested duration")
	})

	t.Run("BgStatusStaysInProgress", func(t *testing.T) {
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockPluginService.EXPECT().GetBgStatus(bgId).Return(bgInProgressStatus, true).AnyTimes()
		mockPluginService.EXPECT().GetTelemetryContext().Return(ctxBefore).Times(1)
		mockPluginService.EXPECT().GetTelemetryFactory().Return(mockTelemetry).Times(1)
		mockPluginService.EXPECT().SetTelemetryContext(gomock.Any()).Times(2)

		start := time.Now()
		conn, err := routing.Apply(nil, hostInfo, props, true, mockPluginService)
		require.NotNil(t, err, "Should return error")
		assert.True(t, strings.Contains(err.Error(), "Blue/Green Deployment switchover is still in progress"))
		assert.Nil(t, conn, "Should not create a connection")

		elapsed := time.Since(start)
		assert.True(t, elapsed >= 45*time.Millisecond, "Should sleep for at least the requested duration")
		assert.True(t, elapsed < 110*time.Millisecond, "Should not sleep much longer than requested. Slept for %d ms.", elapsed.Milliseconds())
	})

	t.Run("BgStatusChanges", func(t *testing.T) {
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockPluginService.EXPECT().GetBgStatus(bgId).Return(bgInProgressStatus, true).Times(2)
		mockPluginService.EXPECT().GetBgStatus(bgId).Return(bgPostStatus, true)
		mockPluginService.EXPECT().GetTelemetryContext().Return(ctxBefore).Times(1)
		mockPluginService.EXPECT().GetTelemetryFactory().Return(mockTelemetry).Times(1)
		mockPluginService.EXPECT().SetTelemetryContext(ctxBefore).Times(2)

		start := time.Now()
		conn, err := routing.Apply(nil, hostInfo, props, true, mockPluginService)
		require.NotNil(t, err, "Should return error")
		assert.True(t, strings.Contains(err.Error(), "Blue/Green Deployment switchover is completed. Continue with connect call."))
		assert.Nil(t, conn, "Should not create a connection")

		elapsed := time.Since(start)
		assert.True(t, elapsed >= 45*time.Millisecond, "Should sleep for at least the requested duration")
		assert.True(t, elapsed < 110*time.Millisecond, "Should not sleep much longer than requested. Slept for %d ms.", elapsed.Milliseconds())
	})
}

func TestSuspendUntilCorrespondingHostFoundConnectRoutingApply(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTelemetry := mock_telemetry.NewMockTelemetryFactory(ctrl)
	mockTelemetryCtx := mock_telemetry.NewMockTelemetryContext(ctrl)

	ctxBefore := context.Background()
	mockTelemetry.EXPECT().
		OpenTelemetryContext(gomock.Any(), telemetry.NESTED, ctxBefore).
		Return(mockTelemetryCtx, ctxBefore).AnyTimes()
	mockTelemetryCtx.EXPECT().CloseContext().AnyTimes()

	hostAndPort := "test-host:5432"
	role := driver_infrastructure.SOURCE
	bgId := "test-bg-deployment-456"
	hostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("test-host").SetPort(5432).Build()

	routing := bg.NewSuspendUntilCorrespondingHostFoundConnectRouting(hostAndPort, role, bgId)
	props := MakeMapFromKeysAndVals(
		property_util.BG_CONNECT_TIMEOUT_MS.Name, "45",
	)
	assert.NotNil(t, routing, "Should create SuspendUntilCorrespondingNodeFoundConnectRouting instance")

	t.Run("NilBgStatus", func(t *testing.T) {
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockPluginService.EXPECT().GetBgStatus(bgId).Return(driver_infrastructure.BlueGreenStatus{}, false).AnyTimes()
		mockPluginService.EXPECT().GetTelemetryContext().Return(ctxBefore).Times(1)
		mockPluginService.EXPECT().GetTelemetryFactory().Return(mockTelemetry).Times(1)
		mockPluginService.EXPECT().SetTelemetryContext(ctxBefore).Times(2)

		start := time.Now()
		conn, err := routing.Apply(nil, hostInfo, props, true, mockPluginService)
		require.NotNil(t, err, "Should return error")
		assert.True(t, strings.Contains(err.Error(), "Blue/Green Deployment status is completed. Continue with 'connect' call. The call was held for"))
		assert.Nil(t, conn, "Should not create a connection")

		elapsed := time.Since(start)
		assert.True(t, elapsed <= 45*time.Millisecond, "Should not sleep for the requested duration. Time elapsed: %d.", elapsed.Milliseconds())
	})

	t.Run("BgStatusCompleted", func(t *testing.T) {
		bgCompletedStatus := driver_infrastructure.NewBgStatus(bgId, driver_infrastructure.COMPLETED, nil, nil, nil, nil)
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockPluginService.EXPECT().GetBgStatus(bgId).Return(bgCompletedStatus, true).AnyTimes()
		mockPluginService.EXPECT().GetTelemetryContext().Return(ctxBefore).Times(1)
		mockPluginService.EXPECT().GetTelemetryFactory().Return(mockTelemetry).Times(1)
		mockPluginService.EXPECT().SetTelemetryContext(ctxBefore).Times(2)

		start := time.Now()
		conn, err := routing.Apply(nil, hostInfo, props, true, mockPluginService)
		require.NotNil(t, err, "Should return error")
		assert.True(t, strings.Contains(err.Error(), "Blue/Green Deployment status is completed. Continue with 'connect' call. The call was held for"))
		assert.Nil(t, conn, "Should not create a connection")

		elapsed := time.Since(start)
		assert.True(t, elapsed <= 45*time.Millisecond, "Should not sleep for the requested duration. Time elapsed: %d.", elapsed.Milliseconds())
	})

	t.Run("TimeoutWaitingForCorrespondingHost", func(t *testing.T) {
		correspondingHosts := utils.NewRWMap[string, utils.Pair[*host_info_util.HostInfo, *host_info_util.HostInfo]]()
		correspondingHosts.Put("test-host", utils.NewPair(hostInfo, &host_info_util.HostInfo{}))
		bgCompletedStatus := driver_infrastructure.NewBgStatus(bgId, driver_infrastructure.POST, nil, nil, nil, correspondingHosts)

		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockPluginService.EXPECT().GetBgStatus(bgId).Return(bgCompletedStatus, true).AnyTimes()
		mockPluginService.EXPECT().GetTelemetryContext().Return(ctxBefore).Times(1)
		mockPluginService.EXPECT().GetTelemetryFactory().Return(mockTelemetry).Times(1)
		mockPluginService.EXPECT().SetTelemetryContext(gomock.Any()).Times(2)

		start := time.Now()
		conn, err := routing.Apply(nil, hostInfo, props, true, mockPluginService)
		require.NotNil(t, err, "Should return error")
		assert.True(t, strings.Contains(err.Error(), "Blue/Green Deployment switchover is still in progress and a corresponding host for 'test-host' is not found"))
		assert.Nil(t, conn, "Should not create a connection")

		elapsed := time.Since(start)
		assert.True(t, elapsed >= 45*time.Millisecond, "Should sleep for at least the requested duration")
	})
	t.Run("FindCorrespondingHost", func(t *testing.T) {
		correspondingHosts := utils.NewRWMap[string, utils.Pair[*host_info_util.HostInfo, *host_info_util.HostInfo]]()
		correspondingHosts.Put("test-host", utils.NewPair(hostInfo, hostInfo))
		bgCompletedStatus := driver_infrastructure.NewBgStatus(bgId, driver_infrastructure.POST, nil, nil, nil, correspondingHosts)

		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockPluginService.EXPECT().GetBgStatus(bgId).Return(bgCompletedStatus, true).AnyTimes()
		mockPluginService.EXPECT().GetTelemetryContext().Return(ctxBefore).Times(1)
		mockPluginService.EXPECT().GetTelemetryFactory().Return(mockTelemetry).Times(1)
		mockPluginService.EXPECT().SetTelemetryContext(gomock.Any()).Times(2)

		start := time.Now()
		conn, err := routing.Apply(nil, hostInfo, props, true, mockPluginService)
		assert.Nil(t, err, "Should not return error")
		assert.Nil(t, conn, "Should not create a connection")

		elapsed := time.Since(start)
		assert.True(t, elapsed <= 45*time.Millisecond, "Should not sleep for the requested duration. Time elapsed: %d.", elapsed.Milliseconds())
	})
}

func TestSuspendExecuteRoutingApply(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTelemetry := mock_telemetry.NewMockTelemetryFactory(ctrl)
	mockTelemetryCtx := mock_telemetry.NewMockTelemetryContext(ctrl)

	ctxBefore := context.Background()
	mockTelemetry.EXPECT().
		OpenTelemetryContext(gomock.Any(), telemetry.NESTED, ctxBefore).
		Return(mockTelemetryCtx, ctxBefore).AnyTimes()
	mockTelemetryCtx.EXPECT().CloseContext().AnyTimes()

	hostAndPort := "test-host:5432"
	role := driver_infrastructure.SOURCE
	bgId := "test-bg-id"
	bgInProgressStatus := driver_infrastructure.NewBgStatus(bgId, driver_infrastructure.IN_PROGRESS, nil, nil, nil, nil)
	bgPostStatus := driver_infrastructure.NewBgStatus(bgId, driver_infrastructure.POST, nil, nil, nil, nil)

	routing := bg.NewSuspendExecuteRouting(hostAndPort, role, bgId)
	props := MakeMapFromKeysAndVals(
		property_util.BG_CONNECT_TIMEOUT_MS.Name, "45",
	)
	assert.NotNil(t, routing, "Should create SuspendExecuteRouting instance")

	methodName := "testMethod"
	methodFunc := func() (any, any, bool, error) {
		return nil, nil, false, nil
	}

	t.Run("NilBgStatus", func(t *testing.T) {
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockPluginService.EXPECT().GetBgStatus(bgId).Return(driver_infrastructure.BlueGreenStatus{}, false).AnyTimes()
		mockPluginService.EXPECT().GetTelemetryContext().Return(ctxBefore).Times(1)
		mockPluginService.EXPECT().GetTelemetryFactory().Return(mockTelemetry).Times(1)
		mockPluginService.EXPECT().SetTelemetryContext(ctxBefore).Times(2)

		start := time.Now()
		result := routing.Apply(nil, props, mockPluginService, methodName, methodFunc)
		assert.False(t, result.IsPresent(), "Should return empty result when no blue/green status")

		elapsed := time.Since(start)
		assert.True(t, elapsed <= 45*time.Millisecond, "Should not sleep for the requested duration")
	})

	t.Run("BgStatusStaysInProgress", func(t *testing.T) {
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockPluginService.EXPECT().GetBgStatus(bgId).Return(bgInProgressStatus, true).AnyTimes()
		mockPluginService.EXPECT().GetTelemetryContext().Return(ctxBefore).Times(1)
		mockPluginService.EXPECT().GetTelemetryFactory().Return(mockTelemetry).Times(1)
		mockPluginService.EXPECT().SetTelemetryContext(gomock.Any()).Times(2)

		start := time.Now()
		result := routing.Apply(nil, props, mockPluginService, methodName, methodFunc)
		assert.True(t, result.IsPresent(), "Should return result with error")
		assert.NotNil(t, result.WrappedErr, "Should return error")
		assert.True(t, strings.Contains(result.WrappedErr.Error(), "Blue/Green Deployment switchover is still in progress"))

		elapsed := time.Since(start)
		assert.True(t, elapsed >= 45*time.Millisecond, "Should sleep for at least the requested duration")
		assert.True(t, elapsed < 110*time.Millisecond, "Should not sleep much longer than requested. Slept for %d ms.", elapsed.Milliseconds())
	})

	t.Run("BgStatusChanges", func(t *testing.T) {
		mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
		mockPluginService.EXPECT().GetBgStatus(bgId).Return(bgInProgressStatus, true).Times(2)
		mockPluginService.EXPECT().GetBgStatus(bgId).Return(bgPostStatus, true)
		mockPluginService.EXPECT().GetTelemetryContext().Return(ctxBefore).Times(1)
		mockPluginService.EXPECT().GetTelemetryFactory().Return(mockTelemetry).Times(1)
		mockPluginService.EXPECT().SetTelemetryContext(ctxBefore).Times(2)

		start := time.Now()
		result := routing.Apply(nil, props, mockPluginService, methodName, methodFunc)
		assert.False(t, result.IsPresent(), "Should return empty result when switchover completes")

		elapsed := time.Since(start)
		assert.True(t, elapsed >= 45*time.Millisecond, "Should sleep for at least the requested duration")
		assert.True(t, elapsed < 110*time.Millisecond, "Should not sleep much longer than requested. Slept for %d ms.", elapsed.Milliseconds())
	})
}

func TestBaseRoutingDelay(t *testing.T) {
	routing := bg.NewBaseRouting("test-host:5432", driver_infrastructure.SOURCE)

	zeroStatus := driver_infrastructure.BlueGreenStatus{}

	start := time.Now()
	routing.Delay(50*time.Millisecond, zeroStatus, nil, "")
	elapsed := time.Since(start)

	assert.True(t, elapsed >= 40*time.Millisecond, "Should sleep for at least the requested duration")
	assert.True(t, elapsed < 100*time.Millisecond, "Should not sleep much longer than requested")
}
