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
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/bg"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestBlueGreenStatusProviderGetMonitoringProperties(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := &driver_infrastructure.MySQLDatabaseDialect{}

	props := MakeMapFromKeysAndVals(
		property_util.BG_INTERVAL_BASELINE_MS.Name, "300000",
		property_util.BG_INTERVAL_INCREASED_MS.Name, "60000",
		property_util.BG_INTERVAL_HIGH_MS.Name, "5000",
		property_util.BG_SWITCHOVER_TIMEOUT_MS.Name, "600000",
		property_util.BG_SUSPEND_NEW_BLUE_CONNECTIONS.Name, "false",
		property_util.BG_PROPERTY_PREFIX+"user", "testuser",
		property_util.BG_PROPERTY_PREFIX+"password", "testpass",
		"normalProp", "normalValue",
	)

	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(&host_info_util.HostInfo{Host: "test-host"}, nil).AnyTimes()

	provider := bg.NewBlueGreenStatusProvider(mockPluginService, props, "test-bg-id")
	assert.NotNil(t, provider)
	provider.ClearMonitors()

	monitoringProps := provider.GetMonitoringProperties()

	// BG prefixed properties should be stripped of prefix
	assert.Equal(t, "testuser", property_util.USER.Get(monitoringProps))
	assert.Equal(t, "testpass", property_util.PASSWORD.Get(monitoringProps))

	// Normal properties should remain
	normalPropVal, _ := monitoringProps.Get("normalProp")
	assert.Equal(t, "normalValue", normalPropVal)

	// BG prefixed properties should be removed from monitoring props
	_, isBGPrefixedUserPresent := monitoringProps.Get(property_util.BG_PROPERTY_PREFIX + "user")
	_, isBGPrefixedPasswordPresent := monitoringProps.Get(property_util.BG_PROPERTY_PREFIX + "password")
	assert.False(t, isBGPrefixedUserPresent)
	assert.False(t, isBGPrefixedPasswordPresent)
}

func TestBlueGreenStatusProviderUpdatePhaseForward(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)

	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(&host_info_util.HostInfo{Host: "test-host"}, nil).AnyTimes()

	provider := bg.NewTestBlueGreenStatusProvider(mockPluginService, nil, "test-bg-id")
	provider.ClearMonitors()

	interimStatus1 := bg.NewTestBlueGreenInterimStatus(driver_infrastructure.CREATED,
		nil, nil, false, false, false)
	interimStatus2 := bg.NewTestBlueGreenInterimStatus(driver_infrastructure.PREPARATION,
		nil, nil, false, false, false)

	provider.UpdatePhase(driver_infrastructure.SOURCE, interimStatus1)
	assert.Equal(t, driver_infrastructure.CREATED, provider.GetLatestStatusPhase())

	provider.UpdatePhase(driver_infrastructure.SOURCE, interimStatus2)
	assert.Equal(t, driver_infrastructure.PREPARATION, provider.GetLatestStatusPhase())
}

func TestBlueGreenStatusProviderUpdatePhaseRollback(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)

	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(&host_info_util.HostInfo{Host: "test-host"}, nil).AnyTimes()

	provider := bg.NewTestBlueGreenStatusProvider(mockPluginService, nil, "test-bg-id")
	provider.ClearMonitors()
	interimStatus1 := bg.NewTestBlueGreenInterimStatus(driver_infrastructure.PREPARATION,
		nil, nil, false, false, false)
	provider.GetInterimStatuses()[driver_infrastructure.SOURCE.GetValue()] = interimStatus1
	provider.UpdatePhase(driver_infrastructure.SOURCE, interimStatus1)
	assert.Equal(t, driver_infrastructure.PREPARATION, provider.GetLatestStatusPhase())

	interimStatus2 := bg.NewTestBlueGreenInterimStatus(driver_infrastructure.CREATED,
		nil, nil, false, false, false)
	provider.UpdatePhase(driver_infrastructure.SOURCE, interimStatus2)
	assert.True(t, provider.GetRollback())
	assert.Equal(t, driver_infrastructure.CREATED, provider.GetLatestStatusPhase())
}

func TestBlueGreenStatusProviderGetStatusOfCreated(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(&host_info_util.HostInfo{Host: "test-host"}, nil).AnyTimes()

	provider := bg.NewTestBlueGreenStatusProvider(mockPluginService, nil, "test-bg-id")
	provider.ClearMonitors()
	status := provider.GetStatusOfCreated()

	assert.Equal(t, "test-bg-id", status.GetBgId())
	assert.Equal(t, driver_infrastructure.CREATED, status.GetCurrentPhase())
	assert.Empty(t, status.GetConnectRoutings())
	assert.Empty(t, status.GetExecuteRoutings())
}

func TestBlueGreenStatusProviderGetStatusOfPreparation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(&host_info_util.HostInfo{Host: "test-host"}, nil).AnyTimes()

	provider := bg.NewTestBlueGreenStatusProvider(mockPluginService, nil, "test-bg-id")
	provider.ClearMonitors()

	status := provider.GetStatusOfPreparation()
	assert.Equal(t, "test-bg-id", status.GetBgId())
	assert.Equal(t, driver_infrastructure.PREPARATION, status.GetCurrentPhase())
	assert.NotNil(t, status.GetConnectRoutings())

	provider.SetPostStatusEndTime(time.Now())
	status = provider.GetStatusOfPreparation()
	assert.Equal(t, "test-bg-id", status.GetBgId())
	assert.Equal(t, driver_infrastructure.COMPLETED, status.GetCurrentPhase(),
		"If expired and not rolling back, assign status of COMPLETED")

	provider.SetRollback(true)
	status = provider.GetStatusOfPreparation()
	assert.Equal(t, "test-bg-id", status.GetBgId())
	assert.Equal(t, driver_infrastructure.CREATED, status.GetCurrentPhase(),
		"If expired and rolling back, assign status of CREATED")
}

func TestBlueGreenStatusProviderGetStatusOfInProgress(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(&host_info_util.HostInfo{Host: "test-host"}, nil).AnyTimes()

	t.Run("NilHostIpAddresses", func(t *testing.T) {
		provider := bg.NewTestBlueGreenStatusProvider(mockPluginService, nil, "test-bg-id")
		provider.ClearMonitors()
		status := provider.GetStatusOfInProgress()
		assert.Equal(t, "test-bg-id", status.GetBgId())
		assert.Equal(t, driver_infrastructure.IN_PROGRESS, status.GetCurrentPhase())
		assert.Equal(t, 1, len(status.GetConnectRoutings()))
		assert.Equal(t, 2, len(status.GetExecuteRoutings()))
	})

	t.Run("NilGetInterimStatuses()", func(t *testing.T) {
		provider := bg.NewTestBlueGreenStatusProvider(mockPluginService, nil, "test-bg-id")
		provider.ClearMonitors()
		provider.GetHostIpAddresses().Put("blue-host", "192.168.1.1")
		provider.GetHostIpAddresses().Put("green-host", "192.168.1.2")
		status := provider.GetStatusOfInProgress()
		assert.Equal(t, "test-bg-id", status.GetBgId())
		assert.Equal(t, driver_infrastructure.IN_PROGRESS, status.GetCurrentPhase())
		assert.Equal(t, 1, len(status.GetConnectRoutings()))
		assert.Equal(t, 4, len(status.GetExecuteRoutings()))
	})

	t.Run("SuspendNewBlueConnectionsWhenInProgressFalse", func(t *testing.T) {
		provider := bg.NewTestBlueGreenStatusProvider(mockPluginService, nil, "test-bg-id")
		provider.ClearMonitors()
		provider.GetHostIpAddresses().Put("blue-host", "192.168.1.1")
		provider.GetHostIpAddresses().Put("green-host", "192.168.1.2")
		provider.GetInterimStatuses()[driver_infrastructure.SOURCE.GetValue()] =
			bg.NewTestBlueGreenInterimStatus(driver_infrastructure.IN_PROGRESS,
				nil, map[string]string{"blue-host": "192.168.1.1"}, false, false, false)
		provider.GetInterimStatuses()[driver_infrastructure.TARGET.GetValue()] =
			bg.NewTestBlueGreenInterimStatus(driver_infrastructure.IN_PROGRESS,
				nil, map[string]string{"green-host": "192.168.1.2"}, false, false, false)
		status := provider.GetStatusOfInProgress()
		assert.Equal(t, "test-bg-id", status.GetBgId())
		assert.Equal(t, driver_infrastructure.IN_PROGRESS, status.GetCurrentPhase())
		assert.Equal(t, 3, len(status.GetConnectRoutings()))
		assert.Equal(t, 5, len(status.GetExecuteRoutings()))
	})

	t.Run("SuspendNewBlueConnectionsWhenInProgressTrue", func(t *testing.T) {
		provider := bg.NewTestBlueGreenStatusProvider(mockPluginService, MakeMapFromKeysAndVals(
			property_util.BG_SUSPEND_NEW_BLUE_CONNECTIONS.Name, "true",
		), "test-bg-id")
		provider.ClearMonitors()
		provider.GetHostIpAddresses().Put("blue-host", "192.168.1.1")
		provider.GetHostIpAddresses().Put("green-host", "192.168.1.2")
		provider.GetInterimStatuses()[driver_infrastructure.SOURCE.GetValue()] =
			bg.NewTestBlueGreenInterimStatus(driver_infrastructure.IN_PROGRESS,
				nil, map[string]string{"blue-host": "192.168.1.1"}, false, false, false)
		provider.GetInterimStatuses()[driver_infrastructure.TARGET.GetValue()] =
			bg.NewTestBlueGreenInterimStatus(driver_infrastructure.IN_PROGRESS,
				nil, map[string]string{"green-host": "192.168.1.2"}, false, false, false)
		status := provider.GetStatusOfInProgress()
		assert.Equal(t, "test-bg-id", status.GetBgId())
		assert.Equal(t, driver_infrastructure.IN_PROGRESS, status.GetCurrentPhase())
		assert.Equal(t, 6, len(status.GetConnectRoutings()))
		assert.Equal(t, 6, len(status.GetExecuteRoutings()))
	})

	t.Run("PastEndTime", func(t *testing.T) {
		provider := bg.NewTestBlueGreenStatusProvider(mockPluginService, nil, "test-bg-id")
		provider.ClearMonitors()
		provider.SetPostStatusEndTime(time.Now())
		status := provider.GetStatusOfInProgress()
		assert.Equal(t, "test-bg-id", status.GetBgId())
		assert.Equal(t, driver_infrastructure.COMPLETED, status.GetCurrentPhase(),
			"If expired and not rolling back, assign status of COMPLETED")
	})

	t.Run("PastEndTimeRollback", func(t *testing.T) {
		provider := bg.NewTestBlueGreenStatusProvider(mockPluginService, nil, "test-bg-id")
		provider.ClearMonitors()
		provider.SetPostStatusEndTime(time.Now())
		provider.SetRollback(true)
		status := provider.GetStatusOfInProgress()
		assert.Equal(t, "test-bg-id", status.GetBgId())
		assert.Equal(t, driver_infrastructure.CREATED, status.GetCurrentPhase(),
			"If expired and rolling back, assign status of CREATED")
	})
}

func TestBlueGreenStatusProviderGetStatusOfPost(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(&host_info_util.HostInfo{Host: "test-host"}, nil).AnyTimes()
	provider := bg.NewTestBlueGreenStatusProvider(mockPluginService, nil, "test-bg-id")
	provider.ClearMonitors()

	status := provider.GetStatusOfPost()

	assert.Equal(t, "test-bg-id", status.GetBgId())
	assert.Equal(t, driver_infrastructure.POST, status.GetCurrentPhase())
	assert.NotNil(t, status.GetConnectRoutings())
	assert.Empty(t, status.GetExecuteRoutings())

	provider.SetPostStatusEndTime(time.Now())
	status = provider.GetStatusOfPost()
	assert.Equal(t, "test-bg-id", status.GetBgId())
	assert.Equal(t, driver_infrastructure.COMPLETED, status.GetCurrentPhase(),
		"If expired and not rolling back, assign status of COMPLETED")

	provider.SetRollback(true)
	status = provider.GetStatusOfPost()
	assert.Equal(t, "test-bg-id", status.GetBgId())
	assert.Equal(t, driver_infrastructure.CREATED, status.GetCurrentPhase(),
		"If expired and rolling back, assign status of CREATED")
}

func TestBlueGreenStatusProviderGetStatusOfCompleted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(&host_info_util.HostInfo{Host: "test-host"}, nil).AnyTimes()
	provider := bg.NewTestBlueGreenStatusProvider(mockPluginService, nil, "test-bg-id")
	provider.ClearMonitors()
	provider.SetBlueDnsUpdateCompleted(true)
	provider.SetGreenDnsRemoved(true)

	status := provider.GetStatusOfCompleted()
	assert.Equal(t, "test-bg-id", status.GetBgId())
	assert.Equal(t, driver_infrastructure.COMPLETED, status.GetCurrentPhase())
	assert.Empty(t, status.GetConnectRoutings())
	assert.Empty(t, status.GetExecuteRoutings())

	provider.SetBlueDnsUpdateCompleted(false)
	status = provider.GetStatusOfCompleted()
	assert.Equal(t, "test-bg-id", status.GetBgId())
	assert.Equal(t, driver_infrastructure.POST, status.GetCurrentPhase(),
		"DNS not updated to reflect completion yet, mark as POST")

	provider.SetBlueDnsUpdateCompleted(true)
	provider.SetGreenDnsRemoved(false)
	status = provider.GetStatusOfCompleted()
	assert.Equal(t, "test-bg-id", status.GetBgId())
	assert.Equal(t, driver_infrastructure.POST, status.GetCurrentPhase(),
		"DNS not updated to reflect completion yet, mark as POST")

	provider.SetPostStatusEndTime(time.Now())
	status = provider.GetStatusOfPost()
	assert.Equal(t, "test-bg-id", status.GetBgId())
	assert.Equal(t, driver_infrastructure.COMPLETED, status.GetCurrentPhase(),
		"If expired and not rolling back, assign status of COMPLETED")

	provider.SetRollback(true)
	status = provider.GetStatusOfPost()
	assert.Equal(t, "test-bg-id", status.GetBgId())
	assert.Equal(t, driver_infrastructure.CREATED, status.GetCurrentPhase(),
		"If expired and rolling back, assign status of CREATED")
}

func TestBlueGreenStatusProviderRegisterIamHost(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(&host_info_util.HostInfo{Host: "test-host"}, nil).AnyTimes()

	provider := bg.NewTestBlueGreenStatusProvider(mockPluginService, nil, "test-bg-id")
	provider.ClearMonitors()

	provider.RegisterIamHost("green-host", "blue-host")
	assert.True(t, provider.IsAlreadySuccessfullyConnected("green-host", "blue-host"))
	assert.False(t, provider.IsAlreadySuccessfullyConnected("green-host", "other-host"))
}

func TestBlueGreenStatusProviderGetWriterHost(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(&host_info_util.HostInfo{Host: "test-host"}, nil).AnyTimes()
	provider := bg.NewTestBlueGreenStatusProvider(mockPluginService, nil, "test-bg-id")
	provider.ClearMonitors()

	// Test with empty interim status
	writerHost := provider.GetWriterHost(driver_infrastructure.SOURCE)
	assert.Nil(t, writerHost)

	// Test with topology containing writer
	writerHostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("writer-host").SetRole(host_info_util.WRITER).Build()
	readerHostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("reader-host").SetRole(host_info_util.READER).Build()

	provider.GetInterimStatuses()[driver_infrastructure.SOURCE.GetValue()] = bg.NewTestBlueGreenInterimStatus(driver_infrastructure.BlueGreenPhase{},
		[]*host_info_util.HostInfo{writerHostInfo, readerHostInfo}, nil, false, false, false)

	writerHost = provider.GetWriterHost(driver_infrastructure.SOURCE)
	assert.NotNil(t, writerHost)
	assert.Equal(t, "writer-host", writerHost.GetHost())
}

func TestBlueGreenStatusProviderGetReaderHosts(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(&host_info_util.HostInfo{Host: "test-host"}, nil).AnyTimes()

	provider := bg.NewTestBlueGreenStatusProvider(mockPluginService, nil, "test-bg-id")
	provider.ClearMonitors()

	// Test with empty interim status
	readerHosts := provider.GetReaderHosts(driver_infrastructure.SOURCE)
	assert.Nil(t, readerHosts)

	// Test with topology containing readers
	writerHostInfo, _ := host_info_util.NewHostInfoBuilder().SetHost("writer-host").SetRole(host_info_util.WRITER).Build()
	readerHostInfo1, _ := host_info_util.NewHostInfoBuilder().SetHost("reader-host-1").SetRole(host_info_util.READER).Build()
	readerHostInfo2, _ := host_info_util.NewHostInfoBuilder().SetHost("reader-host-2").SetRole(host_info_util.READER).Build()

	provider.GetInterimStatuses()[driver_infrastructure.SOURCE.GetValue()] = bg.NewTestBlueGreenInterimStatus(driver_infrastructure.BlueGreenPhase{},
		[]*host_info_util.HostInfo{writerHostInfo, readerHostInfo1, readerHostInfo2}, nil, false, false, false)

	readerHosts = provider.GetReaderHosts(driver_infrastructure.SOURCE)
	assert.NotNil(t, readerHosts)
	assert.Len(t, readerHosts, 2)
	assert.Equal(t, "reader-host-1", readerHosts[0].Host)
	assert.Equal(t, "reader-host-2", readerHosts[1].Host)
}

func TestBlueGreenStatusProviderStoreBlueDnsUpdateTime(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(&host_info_util.HostInfo{Host: "test-host"}, nil).AnyTimes()

	provider := bg.NewTestBlueGreenStatusProvider(mockPluginService, nil, "test-bg-id")
	provider.ClearMonitors()

	// Test storing blue DNS update time
	provider.StoreBlueDnsUpdateTime()

	// Verify the time was stored
	phaseTime, exists := provider.GetPhaseTimeNano().Get("Blue DNS updated")
	assert.True(t, exists)
	assert.True(t, time.Since(phaseTime.Timestamp) < time.Second)
}

func TestBlueGreenStatusProviderStoreGreenDnsRemoveTime(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(&host_info_util.HostInfo{Host: "test-host"}, nil).AnyTimes()

	provider := bg.NewTestBlueGreenStatusProvider(mockPluginService, nil, "test-bg-id")
	provider.ClearMonitors()

	// Test storing green DNS remove time
	provider.StoreGreenDnsRemoveTime()

	// Verify the time was stored
	phaseTime, exists := provider.GetPhaseTimeNano().Get("Green DNS removed")
	assert.True(t, exists)
	assert.True(t, time.Since(phaseTime.Timestamp) < time.Second)
}

func TestBlueGreenStatusProviderStoreGreenTopologyChangeTime(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(&host_info_util.HostInfo{Host: "test-host"}, nil).AnyTimes()

	provider := bg.NewTestBlueGreenStatusProvider(mockPluginService, nil, "test-bg-id")
	provider.ClearMonitors()

	// Test storing green topology change time
	provider.StoreGreenTopologyChangeTime()

	// Verify the time was stored
	phaseTime, exists := provider.GetPhaseTimeNano().Get("Green topology changed")
	assert.True(t, exists)
	assert.True(t, time.Since(phaseTime.Timestamp) < time.Second)
}

func TestBlueGreenStatusProviderStartSwitchoverTimer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(&host_info_util.HostInfo{Host: "test-host"}, nil).AnyTimes()

	props := MakeMapFromKeysAndVals(
		property_util.BG_SWITCHOVER_TIMEOUT_MS.Name, "5000", // 5 seconds
	)

	provider := bg.NewTestBlueGreenStatusProvider(mockPluginService, props, "test-bg-id")
	provider.ClearMonitors()

	// Initially, no timer should be set
	assert.True(t, provider.GetPostStatusEndTime().IsZero())

	// Start the switchover timer
	provider.StartSwitchoverTimer()

	// Verify timer was set
	assert.False(t, provider.GetPostStatusEndTime().IsZero())
	assert.True(t, provider.GetPostStatusEndTime().After(time.Now()))

	// Calling again should not change the timer
	originalTime := provider.GetPostStatusEndTime()
	provider.StartSwitchoverTimer()
	assert.Equal(t, originalTime, provider.GetPostStatusEndTime())
}

func TestBlueGreenStatusProviderUpdateDnsFlags(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(&host_info_util.HostInfo{Host: "test-host"}, nil).AnyTimes()

	provider := bg.NewTestBlueGreenStatusProvider(mockPluginService, nil, "test-bg-id")
	provider.ClearMonitors()

	// Test blue DNS update completion
	interimStatus := bg.NewTestBlueGreenInterimStatus(driver_infrastructure.BlueGreenPhase{},
		nil, nil, true, false, false)

	assert.False(t, provider.GetBlueDnsUpdateCompleted())
	provider.UpdateDnsFlags(driver_infrastructure.SOURCE, interimStatus)
	assert.True(t, provider.GetBlueDnsUpdateCompleted())

	// Test green DNS removal
	interimStatus = bg.NewTestBlueGreenInterimStatus(driver_infrastructure.BlueGreenPhase{},
		nil, nil, false, true, false)

	assert.False(t, provider.GetGreenDnsRemoved())
	provider.UpdateDnsFlags(driver_infrastructure.TARGET, interimStatus)
	assert.True(t, provider.GetGreenDnsRemoved())

	// Test green topology change
	interimStatus = bg.NewTestBlueGreenInterimStatus(driver_infrastructure.BlueGreenPhase{},
		nil, nil, false, false, true)

	assert.False(t, provider.GetGreenTopologyChanged())
	provider.UpdateDnsFlags(driver_infrastructure.TARGET, interimStatus)
	assert.True(t, provider.GetGreenTopologyChanged())
}

func TestBlueGreenStatusProviderAddSubstituteBlueWithIpAddressConnectRouting(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(&host_info_util.HostInfo{Host: "test-host"}, nil).AnyTimes()

	provider := bg.NewTestBlueGreenStatusProvider(mockPluginService, nil, "test-bg-id")
	provider.ClearMonitors()

	// Set up test data
	blueHost, _ := host_info_util.NewHostInfoBuilder().SetHost("blue-host").SetPort(3306).Build()
	provider.GetRoleByHost().Put("blue-host", driver_infrastructure.SOURCE)
	provider.GetCorrespondingHosts().Put("blue-host", utils.NewPair(blueHost, blueHost))
	provider.GetHostIpAddresses().Put("blue-host", "192.168.1.1")
	provider.GetInterimStatuses()[driver_infrastructure.SOURCE.GetValue()] = bg.NewTestBlueGreenInterimStatus(driver_infrastructure.BlueGreenPhase{},
		nil, nil, false, false, false)

	// Test the method
	routing := provider.AddSubstituteBlueWithIpAddressConnectRouting()

	// Verify routing was created
	assert.NotEmpty(t, routing)
	// Should have routing for both host and host:port
	assert.Len(t, routing, 2)
}

func TestBlueGreenStatusProviderCreatePostRouting(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(&host_info_util.HostInfo{Host: "test-host"}, nil).AnyTimes()

	provider := bg.NewTestBlueGreenStatusProvider(mockPluginService, nil, "test-bg-id")
	provider.ClearMonitors()

	// Test with no DNS updates completed
	provider.SetBlueDnsUpdateCompleted(false)
	provider.SetAllGreenHostsChangedName(false)

	// Set up test data
	blueHost, _ := host_info_util.NewHostInfoBuilder().SetHost("blue-host").SetPort(3306).Build()
	greenHost, _ := host_info_util.NewHostInfoBuilder().SetHost("green-host").SetPort(3306).Build()

	provider.GetRoleByHost().Put("blue-host", driver_infrastructure.SOURCE)
	provider.GetCorrespondingHosts().Put("blue-host", utils.NewPair(blueHost, greenHost))
	provider.GetHostIpAddresses().Put("green-host", "192.168.1.2")

	routing := provider.CreatePostRouting()
	assert.NotEmpty(t, routing)

	// Test with DNS updates completed
	provider.SetBlueDnsUpdateCompleted(true)
	provider.SetAllGreenHostsChangedName(true)

	routing = provider.CreatePostRouting()
	assert.Empty(t, routing) // Should be empty when DNS updates are completed
}

func TestBlueGreenStatusProviderResetContextWhenCompleted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(&host_info_util.HostInfo{Host: "test-host"}, nil).AnyTimes()

	provider := bg.NewTestBlueGreenStatusProvider(mockPluginService, nil, "test-bg-id")
	provider.ClearMonitors()

	// Set up completed state
	provider.SetSummaryStatus(driver_infrastructure.NewBgStatus("test-bg-id", driver_infrastructure.COMPLETED, nil, nil, nil, nil))
	provider.SetRollback(false)
	provider.GetPhaseTimeNano().Put("test-phase", bg.PhaseTimeInfo{
		Timestamp: time.Now(),
		Phase:     driver_infrastructure.COMPLETED,
	})

	// Set some state that should be reset
	provider.SetGreenDnsRemoved(true)
	provider.SetGreenTopologyChanged(true)
	provider.SetAllGreenHostsChangedName(true)

	// Test reset
	provider.ResetContextWhenCompleted()

	// Verify state was reset
	assert.False(t, provider.GetRollback())
	assert.False(t, provider.GetGreenDnsRemoved())
	assert.False(t, provider.GetGreenTopologyChanged())
	assert.False(t, provider.GetAllGreenHostsChangedName())
	assert.Equal(t, 0, provider.GetPhaseTimeNano().Size())
}

func TestBlueGreenStatusProviderPutIfAbsentPhaseTime(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockDialect := mock_driver_infrastructure.NewMockBlueGreenDialect(ctrl)
	mockPluginService.EXPECT().GetDialect().Return(mockDialect).AnyTimes()
	mockPluginService.EXPECT().GetCurrentHostInfo().Return(&host_info_util.HostInfo{Host: "test-host"}, nil).AnyTimes()

	provider := bg.NewTestBlueGreenStatusProvider(mockPluginService, nil, "test-bg-id")
	provider.ClearMonitors()

	phase := driver_infrastructure.CREATED

	// Test normal case
	provider.PutIfAbsentPhaseTime("test-phase", phase)
	phaseTime, exists := provider.GetPhaseTimeNano().Get("test-phase")
	assert.True(t, exists)
	assert.Equal(t, phase, phaseTime.Phase)

	// Test rollback case
	provider.SetRollback(true)
	provider.PutIfAbsentPhaseTime("rollback-phase", phase)
	phaseTime, exists = provider.GetPhaseTimeNano().Get("rollback-phase (rollback)")
	assert.True(t, exists)
	assert.Equal(t, phase, phaseTime.Phase)

	// Test that existing entries are not overwritten
	originalTime := phaseTime.Timestamp
	time.Sleep(1 * time.Millisecond) // Ensure different timestamp
	provider.PutIfAbsentPhaseTime("rollback-phase", phase)
	phaseTime, _ = provider.GetPhaseTimeNano().Get("rollback-phase (rollback)")
	assert.Equal(t, originalTime, phaseTime.Timestamp) // Should not change
}
